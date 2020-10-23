when not compileOption("threads"):
  {.error: "This module is available only when --threads:on".}

import locks
import net
import tables
import uri
import os
import streams
import md5
import strutils
import scram/client
import ../bson except `()`
import ./clientbase
import ./errors

type
  SharedChannel[T] = ptr Channel[T]
  SharedLock = ptr Lock

  Mongo* = ref object of MongoBase    ## Mongo client object
    requestLock:                    SharedLock
    pool {.guard: requestLock.}:    seq[LockedSocket]
    threads:                        seq[Thread[(SharedChannel[(int64, seq[Bson])], SharedChannel[string], string, Port)]]
    current:                        int ## Current (possibly) free socket to use

  LockedSocket* = ref object of LockedSocketBase
    id:         int
    reader:     SharedChannel[(int64, seq[Bson])]
    writer:     SharedChannel[string]

proc newSharedChannel[T](): SharedChannel[T] =
  result = cast[SharedChannel[T]](allocShared0(sizeof(Channel[T])))
  open(result[])

proc close[T](ch: var SharedChannel[T]) =
  close(ch[])
  deallocShared(ch)
  ch = nil

proc newSharedLock(): SharedLock =
  result = cast[SharedLock](allocShared0(sizeof(Lock)))
  initLock(result[])

proc delSharedLock(l: var SharedLock) =
  deinitLock(l[])
  deallocShared(l)
  l = nil

template withRequestLock(body: untyped): untyped =
  m.requestLock[].acquire()
  try:
    {.locks: [m.requestLock].}:
      body
  finally:
    m.requestLock[].release()

proc newLockedSocket(): LockedSocket =
  ## Constructor for "locked" socket
  result.new()
  result.init()
  result.reader = newSharedChannel[(int64, seq[Bson])]()
  result.writer = newSharedChannel[string]()

proc handleResponses(args: (SharedChannel[(int64, seq[Bson])], SharedChannel[string], string, Port)) {.thread.}
proc initPool(m: var Mongo, maxConnections: int) =
  m.threads = @[]
  m.threads.setLen(maxConnections)
  m.requestLock = newSharedLock()
  withRequestLock:
    m.pool = newSeq[LockedSocket](maxConnections)
    for i in 0..<maxConnections:
      m.pool[i] = newLockedSocket()
      m.pool[i].id = i
      createThread(m.threads[i], handleResponses, (m.pool[i].reader, m.pool[i].writer, m.host, Port(m.port)))
      m.pool[i].connected = true

proc newMongo*(host: string = "127.0.0.1", port: uint16 = DefaultMongoPort, secure=false, maxConnections=16): Mongo =
  ## Mongo client constructor
  result.new()
  result.init(host, port)
  result.initPool(maxConnections)
  result.current = -1

proc newMongoWithURI*(u: Uri, maxConnections=16): Mongo =
  result.new()
  result.init(u)
  result.initPool(maxConnections)
  result.current = -1

proc newMongoWithURI*(u: string, maxConnections=16): Mongo = newMongoWithURI(parseUri(u), maxConnections)

proc authenticateScramSha1(db: Database[Mongo], username: string, password: string, ls: LockedSocket): bool {.discardable, gcsafe.}
proc acquire*(m: Mongo): LockedSocket =
  ## Retrieves next non-in-use socket for request
  while true:
    withRequestLock:
      for i in 0..m.pool.len():
        m.current = (m.current + 1) mod m.pool.len()
        let s = m.pool[m.current]
        if not s.inuse:
          if not s.authenticated and m.needsAuth:
            s.authenticated = m[m.authDb()].authenticateScramSha1(m.username, m.password, s)
            m.authenticated = s.authenticated
          s.inuse = true
          return s
    sleep(500)

proc release*(m: Mongo, ls: LockedSocket) =
  withRequestLock:
    if ls.inuse:
      ls.inuse = false
    else:
      raise newException(ValueError, "Socket can't be released twice")

method kind*(sm: Mongo): ClientKind = ClientKindSync       ## Sync Mongo client

proc connect*(m: Mongo): bool =
  ## Establish connection with Mongo server
  let s = m.acquire()
  m.release(s)
  result = s.connected

proc newMongoDatabase*(u: Uri): Database[Mongo] {.deprecated.} =
  ## Create new Mongo sync client using URI type
  let m = newMongoWithURI(u)
  if m.connect():
    result = m[u.path.extractFilename()]
    withRequestLock:
      for s in m.pool:
        s.authenticated = result.authenticateScramSha1(m.username, m.password, s)

proc handleResponses(args: (SharedChannel[(int64, seq[Bson])], SharedChannel[string], string, Port)) {.thread.}=
  let (reader, writer, host, port) = args
  let sock = newSocket()
  var needRaise = false
  sock.connect(host, port)
  while true:
    let pcktToSend = writer[].recv()
    if pcktToSend == "":
      break
    if sock.trySend(pcktToSend):
      var data: string = newStringOfCap(4)
      var received: int = sock.recv(data, 4)
      if received == 0:
        needRaise = true
        break
      var stream: Stream = newStringStream(data)

      ## Read data
      let messageLength: int32 = stream.readInt32() - 4
      data = newStringOfCap(messageLength)
      received = sock.recv(data, messageLength)
      if received == 0:
        needRaise = true
        break
      stream = newStringStream(data)

      discard stream.readInt32()                     ## requestId
      discard stream.readInt32()                     ## responseTo
      discard stream.readInt32()                     ## opCode
      let responceFlags = stream.readInt32()         ## responseFlags
      var cursorId = stream.readInt64()              ## cursorID
      discard stream.readInt32()                     ## startingFrom
      let numberReturned: int32 = stream.readInt32() ## numberReturned
      var res: seq[Bson] = @[]

      if (responceFlags and RFCursorNotFound) != 0:
        cursorId = 0
      if numberReturned > 0:
        for i in 0..<numberReturned:
          res.add(newBsonDocument(stream))
      reader[].send((cursorId, res))
  if needRaise:
    raise newException(CommunicationError, "Disconnected from MongoDB server")

proc refresh*(f: Cursor[Mongo], lockedSocket: LockedSocket = nil): seq[Bson] =
  ## Private procedure for performing actual query to Mongo
  if f.isClosed():
    raise newException(CommunicationError, "Cursor can't be closed while requesting")

  var res: string
  let numberToReturn = calcReturnSize(f)
  if f.isClosed():
    return @[]

  let reqID = f.connection().nextRequestId()
  if f.cursorId == 0:
    res = prepareQuery(f, reqID, numberToReturn, f.nskip)
  else:
    res = prepareMore(f, reqID, numberToReturn)

  var ls = lockedSocket
  if ls.isNil:
    ls = f.connection().acquire()
  ls.writer[].send(res)
  let (cursorId, data) = ls.reader[].recv()
  if lockedSocket.isNil:
    f.connection().release(ls)
  if f.cursorId == 0 or (f.queryFlags and TailableCursor) == 0:
    f.cursorId = cursorId
    if cursorId == 0:
      f.close()
  if data.len > 0:
    f.updateCount(data.len.int32)
    for doc in data:
      if doc.contains("$err"):
        if doc["code"].toInt == 50:
          raise newException(OperationTimeout, "Command " & $f & " has timed out")
  elif data.len == 0 and numberToReturn == 1:
    raise newException(NotFound, "No documents matching query were found")
  else:
    discard
  return data

proc one(f: Cursor[Mongo], ls: LockedSocket): Bson =
  # Internal proc used for sending authentication requests on particular socket
  let docs = f.limit(1).refresh(ls)
  if docs.len == 0:
    raise newException(NotFound, "No documents matching query were found")
  return docs[0]

proc authenticateScramSha1(db: Database[Mongo], username: string, password: string, ls: LockedSocket): bool {.discardable, gcsafe.} =
  ## Authenticate connection (sync): using SCRAM-SHA-1 auth method
  if username == "" or password == "":
    return false

  var scramClient = newScramClient[SHA1Digest]()
  let clientFirstMessage = scramClient.prepareFirstMessage(username)

  let requestStart = %*{
    "saslStart": 1'i32,
    "mechanism": "SCRAM-SHA-1",
    "payload": bin(clientFirstMessage),
    "autoAuthorize": 1'i32
  }
  let responseStart = db["$cmd"].makeQuery(requestStart).one(ls)
  ## line to check if connect worked
  if isNil(responseStart) or not isNil(responseStart["code"]): return false #connect failed or auth failure
  db.client.authenticated = true
  let
    responsePayload = binstr(responseStart["payload"])
    passwordDigest = $toMd5("$#:mongo:$#" % [username, password])
    clientFinalMessage = scramClient.prepareFinalMessage(passwordDigest, responsePayload)
  let requestContinue1 = %*{
    "saslContinue": 1'i32,
    "conversationId": toInt32(responseStart["conversationId"]),
    "payload": bin(clientFinalMessage)
  }
  let responseContinue1 =  db["$cmd"].makeQuery(requestContinue1).one(ls)

  if responseContinue1["ok"].toFloat64() == 0.0:
    db.client.authenticated = false
    return false

  if not scramClient.verifyServerFinalMessage(binstr(responseContinue1["payload"])):
      raise newException(Exception, "Server returned an invalid signature.")

  # Depending on how it's configured, Cyrus SASL (which the server uses)
  # requires a third empty challenge.
  if not responseContinue1["done"].toBool():
      let requestContinue2 = %*{
        "saslContinue": 1'i32,
        "conversationId": responseContinue1["conversationId"],
        "payload": ""
      }
      let responseContinue2 = db["$cmd"].makeQuery(requestContinue2).one(ls)
      if not responseContinue2["done"].toBool():
          raise newException(Exception, "SASL conversation failed to complete.")
  return true