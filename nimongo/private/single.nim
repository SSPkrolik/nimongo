when compileOption("threads"):
  {.error: "This module is available only when --threads:off".}

import os
import net
import uri
import streams
import md5
import strutils
import scram/client
import ../bson
import ./clientbase
import ./errors

type
  Mongo* = ref object of MongoBase      ## Mongo client object
    pool:           seq[LockedSocket]

  LockedSocket = ref object of LockedSocketBase
    sock:          Socket


proc newLockedSocket(): LockedSocket =
  ## Constructor for "locked" socket
  result.new()
  result.init()
  result.sock = newSocket()

proc initPool(m: var Mongo) =
  m.pool = newSeq[LockedSocket](1)
  m.pool[0] = newLockedSocket()

proc newMongo*(host: string = "127.0.0.1", port: uint16 = DefaultMongoPort, secure=false, maxConnections=16): Mongo =
  ## Mongo client constructor
  result.new()
  result.init(host, port)
  result.initPool()
  

proc newMongoWithURI*(u: Uri, maxConnections=16): Mongo =
  result.new()
  result.init(u)
  result.initPool()

proc newMongoWithURI*(u: string, maxConnections=16): Mongo = newMongoWithURI(parseUri(u), maxConnections)

proc authenticateScramSha1(db: Database[Mongo], username: string, password: string, ls: LockedSocket): bool {.discardable.}
proc acquire*(m: Mongo): LockedSocket =
  ## Retrieves next non-in-use socket for request
  while true:
    let s = m.pool[0]
    if not s.inuse:
      if not s.connected:
        try:
          s.sock.connect(m.host, Port(m.port))
          s.connected = true
          if m.needsAuth() and not s.authenticated:
            s.authenticated = m[m.authDb()].authenticateScramSha1(m.username, m.password, s)
            m.authenticated = s.authenticated
        except OSError:
          continue
      s.inuse = true
      return s
    sleep(1000)

proc release*(m: Mongo, ls: LockedSocket) =
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
    m.pool[0].authenticated = result.authenticateScramSha1(m.username, m.password, m.pool[0])

proc refresh*(f: Cursor[Mongo], lockedSocket: LockedSocket = nil): seq[Bson] =
  ## Private procedure for performing actual query to Mongo
  template releaseSocket(ls: LockedSocket) =
    if lockedSocket.isNil:
      f.connection.release(ls)
  # Template for disconnection handling
  template handleDisconnect(size: int, ls: LockedSocket) =
    if size == 0:
      ls.connected = false
      releaseSocket(ls)
      raise newException(CommunicationError, "Disconnected from MongoDB server")

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
    ls = f.connection.acquire()
  if ls.sock.trySend(res):
    var data: string = newStringOfCap(4)
    var received: int = ls.sock.recv(data, 4)
    handleDisconnect(received, ls)
    var stream: Stream = newStringStream(data)

    ## Read data
    let messageLength: int32 = stream.readInt32() - 4

    data = newStringOfCap(messageLength)
    received = ls.sock.recv(data, messageLength)
    handleDisconnect(received, ls)
    stream = newStringStream(data)

    discard stream.readInt32()                     ## requestId
    discard stream.readInt32()                     ## responseTo
    discard stream.readInt32()                     ## opCode
    let responceFlags = stream.readInt32()         ## responseFlags
    let cursorId = stream.readInt64()              ## cursorID
    discard stream.readInt32()                     ## startingFrom
    let numberReturned: int32 = stream.readInt32() ## numberReturned

    if f.cursorId == 0 or (f.queryFlags and TailableCursor) == 0:
      f.cursorId = cursorId
      if cursorId == 0:
        f.close()
    if (responceFlags and RFCursorNotFound) != 0:
      f.close()
    if numberReturned > 0:
      f.updateCount(numberReturned)
      for i in 0..<numberReturned:
        let doc = newBsonDocument(stream)
        if doc.contains("$err"):
          if doc["code"].toInt == 50:
            releaseSocket(ls)
            raise newException(OperationTimeout, "Command " & $f & " has timed out")
        result.add(doc)
    elif numberToReturn == 1:
      releaseSocket(ls)
      raise newException(NotFound, "No documents matching query were found")
    else:
      discard
  releaseSocket(ls)

proc one(f: Cursor[Mongo], ls: LockedSocket): Bson =
  # Internal proc used for sending authentication requests on particular socket
  let docs = f.limit(1).refresh(ls)
  if docs.len == 0:
    raise newException(NotFound, "No documents matching query were found")
  return docs[0]

proc authenticateScramSha1(db: Database[Mongo], username: string, password: string, ls: LockedSocket): bool {.discardable.} =
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