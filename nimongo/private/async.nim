import asyncdispatch
import asyncnet
import os
import tables
import uri
import sequtils
import streams
import md5
import strutils
import scram/client
import ../bson except `()`
import ./clientbase
import ./errors

type
    AsyncMongo* = ref object of MongoBase     ## Mongo async client object
        pool:           seq[AsyncLockedSocket]  ## Pool of connections
        current:        int                     ## Current (possibly) free socket to use

    AsyncLockedSocket = ref object of LockedSocketBase
        sock:          AsyncSocket
        queue:         TableRef[int32, tuple[cur: Cursor[AsyncMongo], fut: Future[seq[Bson]]] ]

proc newAsyncLockedSocket(): AsyncLockedSocket =
    ## Constructor for "locked" async socket
    result.new()
    result.init()
    result.sock = newAsyncSocket()
    result.queue = newTable[ int32, tuple[cur: Cursor[AsyncMongo], fut: Future[seq[Bson]]] ]()

proc newAsyncMongo*(host: string = "127.0.0.1", port: uint16 = DefaultMongoPort, maxConnections=16): AsyncMongo =
    ## Mongo asynchrnonous client constructor
    result.new()
    result.init(host, port)
    result.pool = @[]
    for i in 0..<maxConnections:
        result.pool.add(newAsyncLockedSocket())
    result.current = -1

proc newAsyncMongoWithURI*(u: Uri, maxConnections=16): AsyncMongo =
    result.new()
    result.init(u)
    result.pool = @[]
    for i in 0..<maxConnections:
        result.pool.add(newAsyncLockedSocket())
    result.current = -1

proc newAsyncMongoWithURI*(u: string, maxConnections=16): AsyncMongo = newAsyncMongoWithURI(parseUri(u), maxConnections)

proc authenticateScramSha1(db: Database[AsyncMongo], username, password: string, ls: AsyncLockedSocket): Future[bool] {.async.}
proc acquire*(am: AsyncMongo): Future[AsyncLockedSocket] {.async.} =
    ## Retrieves next non-in-use async socket for request
    while true:
        for s in am.pool:
            if not s.inuse:
                s.inuse = true
                if not s.connected:
                    try:
                        await s.sock.connect(am.host, asyncdispatch.Port(am.port))
                        s.connected = true
                        if am.needsAuth() and not s.authenticated:
                            s.authenticated = await am[am.authDb()].authenticateScramSha1(am.username, am.password, s)
                            am.authenticated = s.authenticated
                    except OSError:
                        continue
                return s
        await sleepAsync(1)

proc release*(am: AsyncMongo, als: AsyncLockedSocket) =
    als.inuse = false

method kind*(am: AsyncMongo): ClientKind = ClientKindAsync ## Async Mongo client

proc connect*(am: AsyncMongo): Future[bool] {.async.} =
    ## Establish asynchronous connection with Mongo server
    let s = await am.acquire()
    am.release(s)
    result = s.connected

proc newAsyncMongoDatabase*(u: Uri, maxConnections = 16): Future[Database[AsyncMongo]] {.async, deprecated.} =
    ## Create new AsyncMongo client using URI as string
    let client = newAsyncMongoWithURI(u, maxConnections)
    if await client.connect():
        result = client[u.path.extractFileName()]
        var authenticated = newSeq[Future[bool]]()
        for s in client.pool:
            authenticated.add(result.authenticateScramSha1(client.username, client.password, s))
        let authRes = await all(authenticated)
        client.authenticated = authRes.any() do(x: bool) -> bool: x


proc handleResponses(ls: AsyncLockedSocket): Future[void] {.async.} =
    # Template for disconnection handling
    template handleDisconnect(response: string, sock: AsyncLockedSocket) =
        if response == "":
            ls.connected = false
            ls.inuse = false
            raise newException(CommunicationError, "Disconnected from MongoDB server")

    while ls.queue.len > 0:
        var data: string = await ls.sock.recv(4)
        handleDisconnect(data, ls)

        var stream: Stream = newStringStream(data)
        let messageLength: int32 = stream.readInt32() - 4

        ## Read data
        data = ""
        while data.len < messageLength:
            let chunk: string = await ls.sock.recv(messageLength - data.len)
            handleDisconnect(chunk, ls)
            data &= chunk
        stream = newStringStream(data)


        discard stream.readInt32()                     ## requestID
        let responseTo = stream.readInt32()            ## responseTo
        discard stream.readInt32()                     ## opCode
        let responceFlags = stream.readInt32()         ## responseFlags
        let cursorId = stream.readInt64()              ## cursorID
        discard stream.readInt32()                     ## startingFrom
        let numberReturned: int32 = stream.readInt32() ## numberReturned

        let futCur = ls.queue[responseTo]
        var res: seq[Bson] = @[]

        if futCur.cur.cursorId == 0 or (futCur.cur.queryFlags and TailableCursor) == 0:
            futCur.cur.cursorId = cursorId
            if cursorId == 0:
                futCur.cur.close()
        if (responceFlags and RFCursorNotFound) != 0:
            futCur.cur.close()
        if numberReturned > 0:
            futCur.cur.updateCount(numberReturned)
            for i in 0..<numberReturned:
                let doc = newBsonDocument(stream)
                if doc.contains("$err"):
                    if doc["code"].toInt == 50:
                        ls.queue.del responseTo
                        raise newException(OperationTimeout, "Command " & $futCur.cur & " has timed out")
                res.add(doc)
        ls.queue.del responseTo
        futCur.fut.complete(res)

proc refresh*(f: Cursor[AsyncMongo], lockedSocket: AsyncLockedSocket = nil): Future[seq[Bson]] {.async.} =
    ## Private procedure for performing actual asyncronous query to Mongo
    if f.isClosed():
        raise newException(CommunicationError, "Cursor can't be closed while requesting")
    var ls = lockedSocket
    if ls.isNil:
        ls = await f.connection().acquire()

    let requestId = f.connection().nextRequestId()

    var res: string
    let numberToReturn = f.calcReturnSize()
    if f.isClosed():
        return @[]
    
    if f.cursorId == 0:
        res = prepareQuery(f, requestId, numberToReturn, f.nskip)
    else:
        res = prepareMore(f, requestId, numberToReturn)

    await ls.sock.send(res)
    f.connection().release(ls)
    let response = newFuture[seq[Bson]]("recv")
    ls.queue[requestId] = (f, response)
    if ls.queue.len == 1:
        asyncCheck handleResponses(ls)
    result = await response

proc one(f: Cursor[AsyncMongo], ls: AsyncLockedSocket): Future[Bson] {.async.} =
    # Internal proc used for sending authentication requests on particular socket
    let docs = await f.limit(1).refresh(ls)
    if docs.len == 0:
        raise newException(NotFound, "No documents matching query were found")
    return docs[0]

proc authenticateScramSha1(db: Database[AsyncMongo], username, password: string, ls: AsyncLockedSocket): Future[bool] {.async.} =
    ## Authenticate connection (async) using SCRAM-SHA-1 method on particular socket
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
    let responseStart = await db["$cmd"].makeQuery(requestStart).one(ls)
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
    let responseContinue1 = await db["$cmd"].makeQuery(requestContinue1).one(ls)
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
        let responseContinue2 = await db["$cmd"].makeQuery(requestContinue2).one(ls)
        if not responseContinue2["done"].toBool():
            raise newException(Exception, "SASL conversation failed to complete.")
    return true