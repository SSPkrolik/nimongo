# Required for using _Lock on linux
when hostOs == "linux":
    {.passL: "-pthread".}

import asyncdispatch
import asyncnet
import locks
import oids
import sequtils
import sockets
import streams
import strutils
import tables
import typetraits
import unsigned
import json

import bson

type OperationKind = enum      ## Type of operation performed by MongoDB
  OP_REPLY        =    1'i32 ##
  # OP_MSG        = 1000'i32 ## Deprecated.
  OP_UPDATE       = 2001'i32 ##
  OP_INSERT       = 2002'i32 ## Insert new document into MongoDB
  # RESERVED      = 2003'i32 ## Reserved by MongoDB developers
  OP_QUERY        = 2004'i32 ##
  OP_GET_MORE     = 2005'i32 ##
  OP_DELETE       = 2006'i32 ## Remove documents from MongoDB
  OP_KILL_CURSORS = 2007'i32 ##

type ClientKind* = enum
  ClientKindBase  = 0
  ClientKindSync  = 1
  ClientKindAsync = 2

const
  TailableCursor  = 1'i32 shl 1 ## Leave cursor alive on MongoDB side
  SlaveOk         = 1'i32 shl 2 ## Allow to query replica set slaves
  NoCursorTimeout = 1'i32 shl 4 ##
  AwaitData       = 1'i32 shl 5 ##
  Exhaust         = 1'i32 shl 6 ##
  Partial         = 1'i32 shl 7 ## Get info only from running shards

const
  CursorNotFound     = 1'i32       ## Invalid cursor id in Get More operation
  QueryFailure       = 1'i32 shl 1 ## $err field document is returned
  # ShardConfigState = 1'i32 shl 2 ## (used by mongos)
  AwaitCapable       = 1'i32 shl 3 ## Set when server supports AwaitCapable

converter toInt32*(ok: OperationKind): int32 =
  ## Convert OperationKind ot int32
  return ok.int32

type
  MongoBase* = ref object of RootObj    ## Base Mongo client
    requestId:   int32
    requestLock: Lock
    host:        string
    port:        uint16
    queryFlags:  int32

  Mongo* = ref object of MongoBase      ## Mongo client object
    sock:      Socket

  AsyncMongo* = ref object of MongoBase ## Mongo async client object
    sock:      AsyncSocket

  Database*[T:Mongo|AsyncMongo] = ref object of MongoBase   ## MongoDB database object
    name:   string
    client: T

  Collection*[T:Mongo|AsyncMongo] = ref object of MongoBase ## MongoDB collection object
    name:   string
    db:     Database[T]
    client: T

  Cursor*[T:Mongo|AsyncMongo] = ref object     ## MongoDB cursor: manages queries object lazily
    collection: Collection[T]
    query:      Bson
    fields:     seq[string]
    queryFlags: int32

  NotFound* = object of Exception  ## Raises when querying of one documents returns empty result

# === Private APIs === #

proc nextRequestId(m: Mongo): int32 =
    ## Return next request id for current MongoDB client
    m.requestId = (m.requestId + 1) mod (int32.high - 1'i32)
    return m.requestId

proc newCursor(c: Collection): Cursor =
    ## Private constructor for the Find object. Find acts by taking
    ## client settings (flags) that can be overriden when actual
    ## query is performed.
    result.new
    result.collection = c
    result.fields = @[]
    result.queryFlags = c.client.queryFlags

proc buildMessageHeader(messageLength: int32, requestId: int32, responseTo: int32, opCode: OperationKind): string =
    ## Build Mongo message header as a series of bytes
    return int32ToBytes(messageLength) & int32ToBytes(requestId) & int32ToBytes(responseTo) & int32ToBytes(opCode)

proc buildMessageInsert(flags: int32, fullCollectionName: string): string =
    ## Build Mongo insert messsage
    return int32ToBytes(flags) & fullCollectionName & char(0)

proc buildMessageDelete(flags: int32, fullCollectionName: string): string =
    ## Build Mongo delete message
    return int32ToBytes(0'i32) & fullCollectionName & char(0) & int32ToBytes(flags)

proc buildMessageUpdate(flags: int32, fullCollectionName: string): string =
    ## Build Mongo update message
    return int32ToBytes(0'i32) & fullCollectionName & char(0) & int32ToBytes(flags)

proc buildMessageQuery(flags: int32, fullCollectionName: string, numberToSkip: int32, numberToReturn: int32): string =
    ## Build Mongo query message
    return int32ToBytes(flags) & fullCollectionName & char(0) & int32ToBytes(numberToSkip) & int32ToBytes(numberToReturn)

# === Mongo client API === #

proc newMongo*(host: string = "127.0.0.1", port: uint16 = 27017): Mongo =
    ## Mongo client constructor
    result.new
    result.host = host
    result.port = port
    result.requestID = 0
    result.queryFlags = 0
    result.sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, true)

proc newAsyncMongo*(host: string = "127.0.0.1", port: uint16 = 27017): AsyncMongo =
    ## Mongo asynchrnonous client constructor
    result.new
    result.host = host
    result.port = port
    result.requestID = 0
    result.queryFlags = 0
    result.sock = newAsyncSocket()

method kind*(mb: MongoBase): ClientKind = ClientKindBase   ## Base Mongo client
method kind*(sm: Mongo): ClientKind = ClientKindSync       ## Sync Mongo client
method kind*(am: AsyncMongo): ClientKind = ClientKindAsync ## Async Mongo client

proc tailableCursor*(m: Mongo, enable: bool = true): Mongo {.discardable.} =
    ## Enable/disable tailable behaviour for the cursor (cursor is not
    ## removed immediately after the query)
    result = m
    m.queryFlags = if enable: m.queryFlags or TailableCursor else: m.queryFlags and (not TailableCursor)

proc slaveOk*(m: Mongo, enable: bool = true): Mongo {.discardable.} =
    ## Enable/disable querying from slaves in replica sets
    result = m
    m.queryFlags = if enable: m.queryFlags or SlaveOk else: m.queryFlags and (not SlaveOk)

proc noCursorTimeout*(m: Mongo, enable: bool = true): Mongo {.discardable.} =
    ## Enable/disable cursor idle timeout
    result = m
    m.queryFlags = if enable: m.queryFlags or NoCursorTimeout else: m.queryFlags and (not NoCursorTimeout)

proc awaitData*(m: Mongo, enable: bool = true): Mongo {.discardable.} =
    ## Enable/disable data waiting behaviour (along with tailable cursor)
    result = m
    m.queryFlags = if enable: m.queryFlags or AwaitData else: m.queryFlags and (not AwaitData)

proc exhaust*(m: Mongo, enable: bool = true): Mongo {.discardable.} =
    ## Enable/disabel exhaust flag which forces database to giveaway
    ## all data for the query in form of "get more" packages.
    result = m
    m.queryFlags = if enable: m.queryFlags or Exhaust else: m.queryFlags and (not Exhaust)

proc allowPartial*(m: Mongo, enable: bool = true): Mongo {.discardable} =
    ## Enable/disable allowance for partial data retrieval from mongos when
    ## one or more shards are down.
    result = m
    m.queryFlags = if enable: m.queryFlags or Partial else: m.queryFlags and (not Partial)

proc connect*(m: Mongo): bool =
    ## Connect socket to mongo server
    try:
        m.sock.connect(m.host, sockets.Port(m.port), -1)
    except OSError:
        return false
    return true

proc connect*(am: AsyncMongo): Future[bool] {.async.} =
  ## Establish asynchronous connection with Mongo server
  try:
    await am.sock.connect(am.host, asyncdispatch.Port(am.port))
  except OSError:
    return false
  return true

proc `[]`*[T:Mongo|AsyncMongo](client: T, dbName: string): Database[T] =
    ## Retrieves database from Mongo
    result.new()
    result.name = dbName
    result.client = client

proc `$`*(m: Mongo): string =
    ## Return full DSN for the Mongo connection
    return "mongodb://$#:$#" % [m.host, $m.port]

# === Database API === #

proc `$`*(db: Database): string =
    ## Database name string representation
    return db.name

proc `[]`*(db: Database, collectionName: string): Collection =
    ## Retrieves collection from Mongo Database
    result.new
    result.name = collectionName
    result.client = db.client
    result.db = db

# === Collection API === #

proc `$`*(c: Collection): string =
    ## String representation of collection name
    return c.db.name & "." & c.name

proc insert*(c: Collection, document: Bson): bool {.discardable.} =
  ## Insert new document into MongoDB
  {.locks: [c.client.requestLock].}:
    let
      sdoc = document.bytes()
      msgHeader = buildMessageHeader(int32(21 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_INSERT)

    return c.client.sock.trySend(msgHeader & buildMessageInsert(0, $c) & sdoc)

proc insert*(c: Collection, document: Bson): Future[bool] {.async.} =
  ## Insert new document into MongoDB via async connection
  {.locks: [c.client.requestLock].}:
    let
      sdoc = document.bytes()
      msgHeader = buildMessageHeader(int32(21 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_INSERT)
    try:
      await AsyncSocket(c.client.sock).send(msgHeader & buildMessageInsert(0, $c) & sdoc)
    except OSError:
      return false
    return true

proc insert*(c: Collection, documents: seq[Bson], continueOnError: bool = false): bool {.discardable.} =
    ## Insert several new documents into MongoDB using one request
    assert len(documents) > 0

    var total = 0
    let sdocs: seq[string] = mapIt(documents, string, bytes(it))
    for sdoc in sdocs: inc(total, sdoc.len())

    {.locks: [c.client.requestLock].}:
      let msgHeader = buildMessageHeader(int32(21 + len($c) + total), c.client.nextRequestId(), 0, OP_INSERT)
      return c.client.sock.trySend(msgHeader & buildMessageInsert(if continueOnError: 1 else: 0, $c) & foldl(sdocs, a & b))

proc asyncInsert*(c: Collection, documents: seq[Bson], continueOnError: bool = false): Future[bool] {.async.} =
  ## Insert new document into MongoDB via async connection
  assert len(documents) > 0

  var total = 0
  let sdocs: seq[string] = mapIt(documents, string, bytes(it))
  for sdoc in sdocs: inc(total, sdoc.len())

  {.locks: [c.client.requestLock].}:
    let msgHeader = buildMessageHeader(int32(21 + len($c) + total), c.client.nextRequestId(), 0, OP_INSERT)
    try:
      await c.client.asock.send(msgHeader & buildMessageInsert(if continueOnError: 1 else: 0, $c) & foldl(sdocs, a & b))
    except OSError:
      return false
    return true

proc remove*(c: Collection, selector: Bson): bool {.discardable.} =
  ## Delete documents from MongoDB
  {.locks: [c.client.requestLock].}:
    let
      sdoc = selector.bytes()
      msgHeader = buildMessageHeader(int32(25 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_DELETE)

    return c.client.sock.trySend(msgHeader & buildMessageDelete(0, $c) & sdoc)

proc asyncRemove*(c: Collection, selector: Bson): Future[bool] {.async.} =
  {.locks: [c.client.requestLock].}:
    let
      sdoc = selector.bytes()
      msgHeader = buildMessageHeader(int32(25 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_DELETE)
    try:
      await c.client.asock.send(msgHeader & buildMessageDelete(0, $c) & sdoc)
    except OSError:
      return false
    return true

proc update*(c: Collection, selector: Bson, update: Bson): bool {.discardable.} =
    ## Update MongoDB document[s]
    {.locks: [c.client.requestLock].}:
        let
            ssel = selector.bytes()
            supd = update.bytes()
            msgHeader = buildMessageHeader(int32(25 + len($c) + ssel.len() + supd.len()), c.client.nextRequestId(), 0, OP_UPDATE)

        return c.client.sock.trySend(msgHeader & buildMessageUpdate(0, $c) & ssel & supd)

proc find*(c: Collection, query: Bson, fields: seq[string] = @[]): Cursor =
    ## Create lazy query object to MongoDB that can be actually run
    ## by one of the Find object procedures: `one()` or `all()`.
    result = c.newCursor()
    result.query = query
    result.fields = fields

# === Find API === #

proc tailableCursor*(f: Cursor, enable: bool = true): Cursor {.discardable.} =
    ## Enable/disable tailable behaviour for the cursor (cursor is not
    ## removed immediately after the query)
    result = f
    f.queryFlags = if enable: f.queryFlags or TailableCursor else: f.queryFlags and (not TailableCursor)

proc slaveOk*(f: Cursor, enable: bool = true): Cursor {.discardable.} =
    ## Enable/disable querying from slaves in replica sets
    result = f
    f.queryFlags = if enable: f.queryFlags or SlaveOk else: f.queryFlags and (not SlaveOk)

proc noCursorTimeout*(f: Cursor, enable: bool = true): Cursor {.discardable.} =
    ## Enable/disable cursor idle timeout
    result = f
    f.queryFlags = if enable: f.queryFlags or NoCursorTimeout else: f.queryFlags and (not NoCursorTimeout)

proc awaitData*(f: Cursor, enable: bool = true): Cursor {.discardable.} =
    ## Enable/disable data waiting behaviour (along with tailable cursor)
    result = f
    f.queryFlags = if enable: f.queryFlags or AwaitData else: f.queryFlags and (not AwaitData)

proc exhaust*(f: Cursor, enable: bool = true): Cursor {.discardable.} =
    ## Enable/disabel exhaust flag which forces database to giveaway
    ## all data for the query in form of "get more" packages.
    result = f
    f.queryFlags = if enable: f.queryFlags or Exhaust else: f.queryFlags and (not Exhaust)

proc allowPartial*(f: Cursor, enable: bool = true): Cursor {.discardable.} =
    ## Enable/disable allowance for partial data retrieval from mongo when
    ## on or more shards are down.
    result = f
    f.queryFlags = if enable: f.queryFlags or Partial else: f.queryFlags and (not Partial)

proc skip*(f: Cursor, numDocuments: int): Cursor {.discardable.} =
    ## Specify number of documents from return sequence to skip
    result = f

proc limit*(f: Cursor, numLimit: int): Cursor {.discardable.} =
    ## Specify number of documents to return from database
    result = f

proc prepareQuery(f: Cursor, numberToReturn: int32): string =
  ## Prepare query and request queries for makind OP_QUERY
  var bfields: Bson = initBsonDocument()
  if f.fields.len() > 0:
      for field in f.fields.items():
          bfields = bfields(field, 1'i32)
  let
      squery = f.query.bytes()
      sfields: string = if f.fields.len() > 0: bfields.bytes() else: ""
      msgHeader = buildMessageHeader(int32(29 + len($(f.collection)) + squery.len() + sfields.len()), f.collection.client.nextRequestId(), 0, OP_QUERY)

  return msgHeader & buildMessageQuery(0, $(f.collection), 0 , numberToReturn) & squery & sfields

iterator performFind(f: Cursor, numberToReturn: int32): Bson {.closure.} =
    ## Private procedure for performing actual query to Mongo
    {.locks: [f.collection.client.requestLock].}:
      if f.collection.client.sock.trySend(prepareQuery(f, numberToReturn)):
        var data: string = newStringOfCap(4)
        var received: int = f.collection.client.sock.recv(data, 4)
        var stream: Stream = newStringStream(data)

        ## Read data
        let messageLength: int32 = stream.readInt32()

        data = newStringOfCap(messageLength - 4)
        received = f.collection.client.sock.recv(data, messageLength - 4)
        stream = newStringStream(data)

        let requestID: int32 = stream.readInt32()
        let responseTo: int32 = stream.readInt32()
        let opCode: OperationKind = stream.readInt32().OperationKind
        let responseFlags: int32 = stream.readInt32()
        let cursorID: int64 = stream.readInt64()
        let startingFrom: int32 = stream.readInt32()
        let numberReturned: int32 = stream.readInt32()

        if numberReturned > 0:
          for i in 0..<numberReturned:
            let docSize = stream.readInt32()
            stream.setPosition(stream.getPosition() - 4)
            let sdoc: string = stream.readStr(docSize)
            yield initBsonDocument(sdoc)
        elif numberToReturn == 1:
          raise newException(NotFound, "No documents matching query were found")
        else:
          discard

proc asyncPerformFind(f: Cursor, numberToReturn: int32): Future[seq[Bson]] {.async.} =
  ## Private procedure for performing actual query to Mongo via async client
  {.locks: [f.collection.client.requestLock].}:
    await f.collection.client.asock.send(prepareQuery(f, numberToReturn))
    return @[initBsonDocument()]

proc all*(f: Cursor): seq[Bson] =
    ## Perform MongoDB query and return all matching documents
    result = @[]
    for doc in f.performFind(0):
        result.add(doc)

proc asyncOne(f: Cursor): Future[Bson] {.async.} =
  ## Return one document found by making query to Mongo via async client
  let res = await asyncPerformFind(f, 1)
  return res[0]

proc one*(f: Cursor): Bson =
    ## Perform MongoDB query and return first matching document
    var iter = performFind
    return f.iter(1)

iterator items*(f: Cursor): Bson =
    ## Perform MongoDB query and return iterator for all matching documents
    for doc in f.performFind(0):
        yield doc

proc isMaster*(m: Mongo): bool =
    ## Perform query in order to check if connected Mongo instance is a master
    let db: Database[Mongo] = m["admin"]
    return db["$cmd"].find(B("isMaster", 1)).one()["ismaster"]

proc asyncDrop*(c: Collection): Future[bool] {.async.} =
  ## Drop collection from database via async clinet
  let res = await c.db["$cmd"].find(B("drop", c.name)).asyncOne()
  return res["ok"] == 1.0

proc drop*(c: Collection): bool =
  ## Drop collection from database
  return c.db["$cmd"].find(B("drop", c.name)).one()["ok"] == 1.0

proc count*(c: Collection): int =
    ## Return number of documents in collection
    let x = c.db["$cmd"].find(B("count", c.name)).one()["n"]
    if x.kind == BsonKindInt32:
        return x.toInt32()
    elif x.kind == BsonKindDouble:
        return x.toFloat64.int

proc count*(f: Find): int =
    ## Return number of documents in find query result
    let x = f.collection.db["$cmd"].find(B("count", f.collection.name)("query", f.query)).one()["n"]
    if x.kind == BsonKindInt32:
        return x.toInt32()
    elif x.kind == BsonKindDouble:
        return x.toFloat64().int

when isMainModule:
    import md5

    let
      m: Mongo = newMongo()
      a: Mongo = newAsyncMongo()

    echo("Connecting sync : ", m.connect())
    echo("Connecting async: ", waitFor(a.asyncConnect()))

    discard waitFor(a["db"]["async"].asyncInsert(B("code", js("function identity(x) {return x;}"))))

    let someid = genOid()
    echo("Inserting: ", someid, " ", waitFor(a["db"]["async"].asyncInsert(B("_id", someid)("image", bin("asd")))))
    #echo("Removing: ", someid, " ", waitFor(a["db"]["async"].asyncRemove(B("_id", someid))))

    echo(m["db"]["async"].drop())
