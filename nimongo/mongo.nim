# Required for using _Lock on linux
when hostOs == "linux":
    {.passL: "-pthread".}

import asyncdispatch
import asyncnet
import locks
import net
import oids
import sequtils
import streams
import strutils
import tables
import typetraits
import json

import bson

type OperationKind = enum    ## Type of operation performed by MongoDB
  # OP_REPLY        =    1'i32 ##
  OP_UPDATE         = 2001'i32 ##
  OP_INSERT         = 2002'i32 ## Insert new document into MongoDB
  OP_QUERY          = 2004'i32 ##
  # OP_GET_MORE     = 2005'i32 ##
  OP_DELETE         = 2006'i32 ## Remove documents from MongoDB
  # OP_KILL_CURSORS = 2007'i32 ##

type RemoveKind* = enum ## Type of remove operation
  RemoveSingle          ## Remove single document
  RemoveMultiple        ## Remove multiple documents

type UpdateKind* = enum ## Type of update operation
  UpdateSingle          ## Update single document
  UpdateMultiple        ## Update multiple document

type UpsertKind* = enum ## Indicates if need to make upsert
  Upsert                ## Upsert allowed
  NoUpsert              ## Upsert disallowed

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

##  const
##    CursorNotFound     = 1'i32       ## Invalid cursor id in Get More operation
##    QueryFailure       = 1'i32 shl 1 ## $err field document is returned
##    AwaitCapable       = 1'i32 shl 3 ## Set when server supports AwaitCapable

converter toInt32*(ok: OperationKind): int32 =
  ## Convert OperationKind ot int32
  return ok.int32

type
  MongoBase* = ref object of RootObj    ## Base Mongo client
    requestId:    int32
    host:         string
    port:         uint16
    queryFlags:   int32
    replicas:     seq[tuple[host: string, port: uint16]]

  AsyncLockedSocket = object
    inuse: bool
    sock:  AsyncSocket

  Mongo* = ref object of MongoBase      ## Mongo client object
    requestLock: Lock
    sock:        Socket

  AsyncMongo* = ref object of MongoBase ## Mongo async client object
    current: int                     ## Current (possibly) free socket to use
    pool:    seq[AsyncLockedSocket]  ## Pool of connections

  Database*[T] = ref object of MongoBase   ## MongoDB database object
    name:   string
    client: T

  Collection*[T] = ref object of MongoBase ## MongoDB collection object
    name:   string
    db:     Database[T]
    client: T

  Cursor*[T] = ref object     ## MongoDB cursor: manages queries object lazily
    collection: Collection[T]
    query:      Bson
    fields:     seq[string]
    queryFlags: int32
    nskip:       int32
    nlimit:      int32

  NotFound* = object of Exception   ## Raises when querying of one documents returns empty result

# === Private APIs === #

proc nextRequestId(mb: MongoBase): int32 =
    ## Return next request id for current MongoDB client
    mb.requestId = (mb.requestId + 1) mod (int32.high - 1'i32)
    return mb.requestId

proc newCursor[T](c: Collection[T]): Cursor[T] =
    ## Private constructor for the Find object. Find acts by taking
    ## client settings (flags) that can be overriden when actual
    ## query is performed.
    result.new
    result.collection = c
    result.fields = @[]
    result.queryFlags = c.client.queryFlags
    result.nskip = 0
    result.nlimit = 0

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
    result.sock = newSocket()
    result.replicas = @[]

proc newAsyncLockedSocket(): AsyncLockedSocket =
  ## Constructor for "locked" async socket
  return AsyncLockedSocket(
    inuse: false,
    sock: newAsyncSocket()
  )

proc newAsyncMongo*(host: string = "127.0.0.1", port: uint16 = 27017, maxConnections=16): AsyncMongo =
    ## Mongo asynchrnonous client constructor
    result.new
    result.host = host
    result.port = port
    result.requestID = 0
    result.queryFlags = 0
    result.pool = @[]
    for i in 0..<maxConnections:
      result.pool.add(newAsyncLockedSocket())
    result.current = -1
    result.replicas = @[]

proc next(am: AsyncMongo): Future[AsyncLockedSocket] {.async.} =
  ## Retrieves next non-in-use async socket for request
  while true:
    for _ in 0..<am.pool.len():
      am.current = (am.current + 1) mod am.pool.len()
      if not am.pool[am.current].inuse:
        return am.pool[am.current]
    await sleepAsync(1)

proc replica*[T:Mongo|AsyncMongo](mb: T, nodes: seq[tuple[host: string, port: uint16]]) =
  for node in nodes:
    when T is Mongo:
      mb.replicas.add((host: node.host, port: sockets.Port(node.port)))
    when T is AsyncMongo:
      mb.replicas.add((host: node.host, port: asyncnet.Port(node.port)))

method kind*(mb: MongoBase): ClientKind {.base.} = ClientKindBase   ## Base Mongo client
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
        m.sock.connect(m.host, net.Port(m.port), -1)
    except OSError:
        return false
    return true

proc connect*(am: AsyncMongo): Future[bool] {.async.} =
  ## Establish asynchronous connection with Mongo server
  for ls in am.pool.items():
    try:
      await ls.sock.connect(am.host, asyncdispatch.Port(am.port))
    except OSError:
      continue
  return true

proc `[]`*[T:Mongo|AsyncMongo](client: T, dbName: string): Database[T] =
    ## Retrieves database from Mongo
    result.new()
    result.name = dbName
    result.client = client

proc `$`*[T:Mongo|AsyncMongo](m: T): string =
    ## Return full DSN for the Mongo connection
    return "mongodb://$#:$#" % [m.host, $m.port]

# === Database API === #

proc `$`*(db: Database): string =
    ## Database name string representation
    return db.name

proc `[]`*[T:Mongo|AsyncMongo](db: Database[T], collectionName: string): Collection[T] =
    ## Retrieves collection from Mongo Database
    result.new()
    result.name = collectionName
    result.client = db.client
    result.db = db

# === Collection API === #

proc `$`*(c: Collection): string =
    ## String representation of collection name
    return c.db.name & "." & c.name

proc insert*(c: Collection[Mongo], document: Bson): bool {.discardable.} =
  ## Insert new document into MongoDB
  {.locks: [c.client.requestLock].}:
    let
      sdoc = document.bytes()
      msgHeader = buildMessageHeader(int32(21 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_INSERT)

    return c.client.sock.trySend(msgHeader & buildMessageInsert(0, $c) & sdoc)

proc insert*(c: Collection[AsyncMongo], document: Bson): Future[bool] {.async.} =
  ## Insert new document into MongoDB via async connection
  let
    sdoc = document.bytes()
    msgHeader = buildMessageHeader(int32(21 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_INSERT)
  var ls: AsyncLockedSocket = await c.client.next()
  try:
    ls.inuse = true
    await ls.sock.send(msgHeader & buildMessageInsert(0, $c) & sdoc)
    ls.inuse = false
  except OSError:
    return false
  return true

proc insert*(c: Collection[Mongo], documents: seq[Bson], continueOnError: bool = false): bool {.discardable.} =
  ## Insert several new documents into MongoDB using one request
  assert len(documents) > 0
  var
    total = 0
    sdocs: seq[string] = @[]
  for d in documents.items(): sdocs.add(bytes(d))
  for sdoc in sdocs: inc(total, sdoc.len())

  let msgHeader = buildMessageHeader(int32(21 + len($c) + total), c.client.nextRequestId(), 0, OP_INSERT)
  return c.client.sock.trySend(msgHeader & buildMessageInsert(if continueOnError: 1 else: 0, $c) & foldl(sdocs, a & b))

proc insert*(c: Collection[AsyncMongo], documents: seq[Bson], continueOnError: bool = false): Future[bool] {.async.} =
  ## Insert new document into MongoDB via async connection
  assert len(documents) > 0
  var
    total = 0
    ls = await c.client.next()
  let sdocs: seq[string] = mapIt(documents, string, bytes(it))
  for sdoc in sdocs: inc(total, sdoc.len())

  let msgHeader = buildMessageHeader(int32(21 + len($c) + total), c.client.nextRequestId(), 0, OP_INSERT)
  try:
    ls.inuse = true
    await ls.sock.send(msgHeader & buildMessageInsert(if continueOnError: 1 else: 0, $c) & foldl(sdocs, a & b))
    ls.inuse = false
  except OSError:
    return false
  return true

proc remove*(c: Collection[Mongo], selector: Bson, mode: RemoveKind): bool {.discardable.} =
  ## Delete document[s] from MongoDB
  {.locks: [c.client.requestLock].}:
    let
      sdoc = selector.bytes()
      msgHeader = buildMessageHeader(int32(25 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_DELETE)

    return c.client.sock.trySend(msgHeader & buildMessageDelete(if mode == RemoveMultiple: 0 else: 1, $c) & sdoc)

proc remove*(c: Collection[AsyncMongo], selector: Bson, mode: RemoveKind): Future[bool] {.async.} =
  ## Delete document[s] from MongoDB via asyn connection
  var ls = await c.client.next()
  let
    sdoc = selector.bytes()
    msgHeader = buildMessageHeader(int32(25 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_DELETE)
  try:
    ls.inuse = true
    await ls.sock.send(msgHeader & buildMessageDelete(if mode == RemoveMultiple: 0 else: 1, $c) & sdoc)
    ls.inuse = false
  except OSError:
    return false
  return true

proc update*(c: Collection[Mongo], selector: Bson, update: Bson, mode: UpdateKind, upsert: UpsertKind): bool {.discardable.} =
  ## Update MongoDB document[s]
  {.locks: [c.client.requestLock].}:
    let
      ssel = selector.bytes()
      supd = update.bytes()
      msgHeader = buildMessageHeader(int32(25 + len($c) + ssel.len() + supd.len()), c.client.nextRequestId(), 0, OP_UPDATE)
    var flags: int32 = 0

    if mode == UpdateMultiple:
      flags = flags or (1'i32 shl 1)
    if upsert == Upsert:
      flags = flags or (1'i32)

    return c.client.sock.trySend(msgHeader & buildMessageUpdate(flags, $c) & ssel & supd)

proc update*(c: Collection[AsyncMongo], selector: Bson, update: Bson, mode: UpdateKind, upsert: UpsertKind): Future[bool] {.async.} =
  ## Update MongoDB document[s] via async connection
  var ls = await c.client.next()
  let
    ssel = selector.bytes()
    supd = update.bytes()
    msgHeader = buildMessageHeader(int32(25 + len($c) + ssel.len() + supd.len()), c.client.nextRequestId(), 0, OP_UPDATE)
  var flags: int32 = 0

  if mode == UpdateMultiple:
    flags = flags or (1'i32 shl 1)
  if upsert == Upsert:
    flags = flags or (1'i32)

  try:
    ls.inuse = true
    await ls.sock.send(msgHeader & buildMessageUpdate(flags, $c) & ssel & supd)
    ls.inuse = false
  except OSError:
    return false
  return true

proc find*[T:Mongo|AsyncMongo](c: Collection[T], query: Bson, fields: seq[string] = @[]): Cursor[T] =
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

proc skip*[T:Mongo|AsyncMongo](f: Cursor[T], numSkip: int32): Cursor[T] {.discardable.} =
    ## Specify number of documents from return sequence to skip
    result = f
    result.nskip = numSkip

proc limit*[T:Mongo|AsyncMongo](f: Cursor[T], numLimit: int32): Cursor[T] {.discardable.} =
    ## Specify number of documents to return from database
    result = f
    result.nlimit = numLimit

proc prepareQuery(f: Cursor, numberToReturn: int32, numberToSkip: int32): string =
  ## Prepare query and request queries for makind OP_QUERY
  var bfields: Bson = initBsonDocument()
  if f.fields.len() > 0:
      for field in f.fields.items():
          bfields = bfields(field, 1'i32)
  let
      squery = f.query.bytes()
      sfields: string = if f.fields.len() > 0: bfields.bytes() else: ""
      msgHeader = buildMessageHeader(int32(29 + len($(f.collection)) + squery.len() + sfields.len()), f.collection.client.nextRequestId(), 0, OP_QUERY)

  return msgHeader & buildMessageQuery(0, $(f.collection), numberToSkip , numberToReturn) & squery & sfields

iterator performFind(f: Cursor[Mongo], numberToReturn: int32, numberToSkip: int32): Bson {.closure.} =
  ## Private procedure for performing actual query to Mongo
  {.locks: [f.collection.client.requestLock].}:
    if f.collection.client.sock.trySend(prepareQuery(f, numberToReturn, numberToSkip)):
      var data: string = newStringOfCap(4)
      var received: int = f.collection.client.sock.recv(data, 4)
      var stream: Stream = newStringStream(data)

      ## Read data
      let messageLength: int32 = stream.readInt32()

      data = newStringOfCap(messageLength - 4)
      received = f.collection.client.sock.recv(data, messageLength - 4)
      stream = newStringStream(data)

      discard stream.readInt32()                     ## requestId
      discard stream.readInt32()                     ## responseTo
      discard stream.readInt32()                     ## opCode
      discard stream.readInt32()                     ## responseFlags
      discard stream.readInt64()                     ## cursorID
      discard stream.readInt32()                     ## startingFrom
      let numberReturned: int32 = stream.readInt32() ## numberReturned

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

proc performFindAsync(f: Cursor[AsyncMongo], numberToReturn: int32, numberToSkip: int32): Future[seq[Bson]] {.async.} =
  ## Private procedure for performing actual query to Mongo via async client
  var ls = await f.collection.client.next()

  ls.inuse = true
  await ls.sock.send(prepareQuery(f, numberToReturn, numberToSkip))
  ## Read Length
  var
    data: string = await ls.sock.recv(4)
    stream: Stream = newStringStream(data)
  let messageLength: int32 = stream.readInt32()

  ## Read data
  data = newStringOfCap(messageLength - 4)
  data = await ls.sock.recv(messageLength - 4)

  ls.inuse = false

  stream = newStringStream(data)

  discard stream.readInt32()                     ## requestId
  discard stream.readInt32()                     ## responseTo
  discard stream.readInt32()                     ## opCode
  discard stream.readInt32()                     ## responseFlags
  discard stream.readInt64()                     ## cursorID
  discard stream.readInt32()                     ## startingFrom
  let numberReturned: int32 = stream.readInt32() ## numberReturned

  result = @[]

  if numberReturned > 0:
    for i in 0..<numberReturned:
      let docSize = stream.readInt32()
      stream.setPosition(stream.getPosition() - 4)
      let sdoc: string = stream.readStr(docSize)
      result.add(initBsonDocument(sdoc))
    return
  elif numberToReturn == 1:
    raise newException(NotFound, "No documents matching query were found")

proc all*(f: Cursor[Mongo]): seq[Bson] =
  ## Perform MongoDB query and return all matching documents
  result = @[]
  for doc in f.performFind(f.nlimit, f.nskip):
    result.add(doc)

proc all*(f: Cursor[AsyncMongo]): Future[seq[Bson]] {.async.} =
  ## Perform MongoDB query asynchronously and return all matching documents.
  result = await f.performFindAsync(f.nlimit, f.nskip)

proc one*(f: Cursor[Mongo]): Bson =
    ## Perform MongoDB query and return first matching document
    var iter = performFind
    return f.iter(1, f.nskip)

proc one*(f: Cursor[AsyncMongo]): Future[Bson] {.async.} =
  ## Perform MongoDB query asynchronously and return first matching document.
  let docs = await f.performFindAsync(1, f.nskip)
  return docs[0]

iterator items*(f: Cursor): Bson =
  ## Perform MongoDB query and return iterator for all matching documents
  for doc in f.performFind(f.nlimit, f.nskip):
      yield doc

proc isMaster*(sm: Mongo): bool =
  ## Perform query in order to check if connected Mongo instance is a master
  return sm["admin"]["$cmd"].find(B("isMaster", 1)).one()["ismaster"]

proc isMaster*(am: AsyncMongo): Future[bool] {.async.} =
  ## Perform query in order to check if ocnnected Mongo instance is a master
  ## via async connection.
  let response = await am["admin"]["$cmd"].find(B("isMaster", 1)).one()
  return response["ismaster"]

proc listDatabases*(sm: Mongo): seq[string] =
  ## Return list of databases on the server
  let response = sm["admin"]["$cmd"].find(B("listDatabases", 1)).one()
  if response["ok"] == 0.0:
    return @[]
  elif response["ok"] == 1.0:
    result = @[]
    for db in response["databases"].items():
      result.add(db["name"].toString())
  else:
    raise new(Exception)

proc listDatabases*(am: AsyncMongo): Future[seq[string]] {.async.} =
  ## Return list of databases on the server via async client
  let response = await am["admin"]["$cmd"].find(B("listDatabases", 1)).one()
  if response["ok"] == 0.0:
    return @[]
  elif response["ok"] == 1.0:
    result = @[]
    for db in response["databases"].items():
      result.add(db["name"].toString())
  else:
    raise new(Exception)

proc listCollections*(db: Database[Mongo], filter: Bson = initBsonDocument()): seq[string] =
  ## List collections inside specified database
  let response = db["$cmd"].find(B("listCollections", 1'i32)).one()
  if response["ok"] == 0.0:
    return @[]

proc drop*(db: Database[Mongo]): bool =
  ## Drop database from server
  let response = db["$cmd"].find(B("dropDatabase", 1)).one()
  return response["ok"] == 1.0

proc drop*(db: Database[AsyncMongo]): Future[bool] {.async.} =
  ## Drop database from server via async connection
  let response = await db["$cmd"].find(B("dropDatabase", 1)).one()
  return response["ok"] == 1.0

proc drop*(c: Collection[Mongo]): tuple[ok: bool, message: string] =
  ## Drop collection from database
  let response = c.db["$cmd"].find(B("drop", c.name)).one()
  let ok = response["ok"] == 1.0
  return (ok: ok, message: if ok: "" else: response["errmsg"])

proc drop*(c: Collection[AsyncMongo]): Future[tuple[ok: bool, message: string]] {.async.} =
  ## Drop collection from database via async clinet
  let response = await c.db["$cmd"].find(B("drop", c.name)).one()
  let ok = response["ok"] == 1.0
  return (ok: ok, message: if ok: "" else: response["errmsg"])

proc count*(c: Collection[Mongo]): int =
  ## Return number of documents in collection
  let x = c.db["$cmd"].find(B("count", c.name)).one()["n"]
  if x.kind == BsonKindInt32:
    return x.toInt32()
  elif x.kind == BsonKindDouble:
    return x.toFloat64.int

proc count*(c: Collection[AsyncMongo]): Future[int] {.async.} =
  ## Return number of documents in collection via async client
  let
    res = await c.db["$cmd"].find(B("count", c.name)).one()
    x = res["n"]
  if x.kind == BsonKindInt32:
    return x.toInt32()
  elif x.kind == BsonKindDouble:
    return x.toFloat64.int

proc count*(f: Cursor[Mongo]): int =
  ## Return number of documents in find query result
  let x = f.collection.db["$cmd"].find(B("count", f.collection.name)("query", f.query)).one()["n"]
  if x.kind == BsonKindInt32:
    return x.toInt32()
  elif x.kind == BsonKindDouble:
    return x.toFloat64().int

proc count*(f: Cursor[AsyncMongo]): Future[int] {.async.} =
  ## Return number of document in find query result via async connection
  let
    response = await f.collection.db["$cmd"].find(B("count", f.collection.name)("query", f.query)).one()
    x = response["n"]
  if x.kind == BsonKindInt32:
    return x.toInt32()
  elif x.kind == BsonKindDouble:
    return x.toFloat64().int

proc unique*(f: Cursor[Mongo], key: string): seq[string] =
  ## Force cursor to return only distinct documents by specified field.
  ## Corresponds to '.distinct()' MongoDB command. If Nim we use 'unique'
  ## because 'distinct' is Nim's reserved keyword.
  let x = f.collection.db["$cmd"].find(B("distinct", f.collection.name)("query", f.query)("key", key)).one()
  result = @[]
  if x["ok"] == 1.0:
    for item in x["values"].items():
      result.add(item.toString())

proc unique*(f: Cursor[AsyncMongo], key: string): Future[seq[string]] {.async.} =
  ## Force cursor to return only distinct documents by specified field.
  ## Corresponds to '.distinct()' MongoDB command. If Nim we use 'unique'
  ## because 'distinct' is Nim's reserved keyword.
  let
    response = await f.collection.db["$cmd"].find(B("distinct", f.collection.name)("query", f.query)("key", key)).one()
    ok = response["ok"]
    va = response["values"]
  result = @[]
  if ok == 1.0:
    for item in va.items():
      result.add(item.toString())
