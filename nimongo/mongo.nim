# Required for using _Lock on linux
when hostOs == "linux":
    {.passL: "-pthread".}

import asyncdispatch
import asyncnet
import base64
import locks
import random
import md5
import net
import oids
import sequtils
import streams
import strutils
import tables
import typetraits
import times
import uri
import os

import bson except `()`
import timeit

import pbkdf2
import sha1, hmac

import auth
import clientbase
import errors
import proto
import reply
import writeconcern

randomize()

export auth
export clientbase except nextRequestId, init
export errors
export reply
export writeconcern

type
  Mongo* = ref object of MongoBase      ## Mongo client object
    requestLock:   Lock
    sock:          Socket
    authenticated*: bool

  AsyncLockedSocket = ref object
    inuse:         bool
    authenticated: bool
    connected:     bool
    sock:          AsyncSocket

  AsyncMongo* = ref object of MongoBase ## Mongo async client object
    current: int                     ## Current (possibly) free socket to use
    pool:    seq[AsyncLockedSocket]  ## Pool of connections

  Database*[T] = ref object of MongoBase   ## MongoDB database object
    name:   string
    client*: T

  CollectionInfo* = object  ## Collection information (for manual creation)
    disableIdIndex*: bool
    forceIdIndex*: bool
    capped: bool
    maxBytes: int
    maxDocs: int

  Collection*[T] = ref object of MongoBase ## MongoDB collection object
    name:   string
    db:     Database[T]
    client: T

  Cursor*[T] = ref object     ## MongoDB cursor: manages queries object lazily
    collection: Collection[T]
    query:      Bson
    fields:     seq[string]
    queryFlags: int32
    nskip:      int32
    nlimit:     int32
    sorting:    Bson

# === Mongo client API === #

proc newMongo*(host: string = "127.0.0.1", port: uint16 = DefaultMongoPort, secure=false): Mongo =
    ## Mongo client constructor
    result.new
    result.init(host, port)
    result.sock = newSocket()
    result.authenticated = false

proc newMongoWithURI*(u: Uri): Mongo =
    result.new
    result.init(u)
    result.sock = newSocket()

proc newMongoWithURI*(u: string): Mongo = newMongoWithURI(parseUri(u))

proc newAsyncLockedSocket(): AsyncLockedSocket =
  ## Constructor for "locked" async socket
  return AsyncLockedSocket(
    inuse:         false,
    authenticated: false,
    connected:     false,
    sock:          newAsyncSocket()
  )

proc newAsyncMongo*(host: string = "127.0.0.1", port: uint16 = DefaultMongoPort, maxConnections=16): AsyncMongo =
    ## Mongo asynchrnonous client constructor
    result.new
    result.init(host, port)
    result.pool = @[]
    for i in 0..<maxConnections:
      result.pool.add(newAsyncLockedSocket())
    result.current = -1

proc newAsyncMongoWithURI*(u: Uri, maxConnections=16): AsyncMongo =
    result.new
    result.init(u)
    result.pool = @[]
    for i in 0..<maxConnections:
      result.pool.add(newAsyncLockedSocket())
    result.current = -1

proc newAsyncMongoWithURI*(u: string): AsyncMongo = newAsyncMongoWithURI(parseUri(u))

proc next(am: AsyncMongo): Future[AsyncLockedSocket] {.async.} =
  ## Retrieves next non-in-use async socket for request
  while true:
    for _ in 0..<am.pool.len():
      am.current = (am.current + 1) mod am.pool.len()
      if not am.pool[am.current].inuse:
        if not am.pool[am.current].connected:
          try:
            await am.pool[am.current].sock.connect(am.host, asyncdispatch.Port(am.port))
            am.pool[am.current].connected = true
          except OSError:
            continue
        am.pool[am.current].inuse = true
        return am.pool[am.current]
    await sleepAsync(1)

proc replica*[T:Mongo|AsyncMongo](mb: T, nodes: seq[tuple[host: string, port: uint16]]) =
  for node in nodes:
    when T is Mongo:
      mb.replicas.add((host: node.host, port: sockets.Port(node.port)))
    when T is AsyncMongo:
      mb.replicas.add((host: node.host, port: asyncnet.Port(node.port)))

method kind*(sm: Mongo): ClientKind = ClientKindSync       ## Sync Mongo client
method kind*(am: AsyncMongo): ClientKind = ClientKindAsync ## Async Mongo client

proc connect*(am: AsyncMongo): Future[bool] {.async.} =
  ## Establish asynchronous connection with Mongo server
  for ls in am.pool.items():
    try:
      await ls.sock.connect(am.host, asyncdispatch.Port(am.port))
      ls.connected = true
    except OSError:
      continue
  return any(am.pool, proc(item: AsyncLockedSocket): bool = item.connected)

proc `[]`*[T:Mongo|AsyncMongo](client: T, dbName: string): Database[T] =
    ## Retrieves database from Mongo
    result.new()
    result.name = dbName
    result.client = client

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

proc makeQuery[T:Mongo|AsyncMongo](c: Collection[T], query: Bson, fields: seq[string] = @[]): Cursor[T] =
  ## Create lazy query object to MongoDB that can be actually run
  ## by one of the Find object procedures: `one()` or `all()`.
  result = c.newCursor()
  result.query = query
  result.fields = fields

proc find*[T:Mongo|AsyncMongo](c: Collection[T], filter: Bson, fields: seq[string] = @[]): Cursor[T] =
  ## Find query
  result = c.newCursor()
  result.query = %*{
    "$query": filter
  }
  result.fields = fields

# === Find API === #

proc orderBy*(f: Cursor, order: Bson): Cursor =
  ## Add sorting setting to query
  result = f
  f.query["$orderby"] = order

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
  ## Prepare query and request queries for making OP_QUERY
  var bfields: Bson = initBsonDocument()
  if f.fields.len() > 0:
      for field in f.fields.items():
          bfields[field] = 1'i32
  let squery = f.query.bytes()
  let sfields: string = if f.fields.len() > 0: bfields.bytes() else: ""

  result = ""
  let colName = $f.collection
  buildMessageHeader(int32(29 + colName.len + squery.len + sfields.len),
    f.collection.client.nextRequestId(), 0, result)

  buildMessageQuery(0, colName, numberToSkip , numberToReturn, result)
  result &= squery
  result &= sfields

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
          yield initBsonDocument(stream)
      elif numberToReturn == 1:
        raise newException(NotFound, "No documents matching query were found")
      else:
        discard

proc performFindAsync(f: Cursor[AsyncMongo], numberToReturn: int32, numberToSkip: int32): Future[seq[Bson]] {.async.} =
  ## Perform asynchronouse OP_QUERY operation to MongoDB.

  # Template for disconnection handling
  template handleDisconnect(response: var string, sock: AsyncLockedSocket) =
    if response == "":
      ls.connected = false
      ls.inuse = false
      raise newException(CommunicationError, "Disconnected from MongoDB server")

  ## Private procedure for performing actual query to Mongo via async client
  var ls: AsyncLockedSocket = await f.collection.client.next()

  await ls.sock.send(prepareQuery(f, numberToReturn, numberToSkip))
  
  # Read reply length
  var data: string = await ls.sock.recv(4)
  handleDisconnect(data, ls)

  var stream: Stream = newStringStream(data)
  let messageLength: int32 = stream.readInt32()

  ## Read data
  data = ""
  while data.len < messageLength - 4:
    var chunk: string = await ls.sock.recv(messageLength - 4)
    handleDisconnect(chunk, ls)
    data = data & chunk

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
      result.add(initBsonDocument(stream))
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
  return sm["admin"]["$cmd"].makeQuery(%*{"isMaster": 1}).one()["ismaster"]

proc isMaster*(am: AsyncMongo): Future[bool] {.async.} =
  ## Perform query in order to check if ocnnected Mongo instance is a master
  ## via async connection.
  let response = await am["admin"]["$cmd"].makeQuery(%*{"isMaster": 1}).one()
  return response["ismaster"]

proc listDatabases*(sm: Mongo): seq[string] =
  ## Return list of databases on the server
  let response = sm["admin"]["$cmd"].makeQuery(%*{"listDatabases": 1}).one()
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
  let response = await am["admin"]["$cmd"].makeQuery(%*{"listDatabases": 1}).one()
  if response["ok"] == 0.0:
    return @[]
  elif response["ok"] == 1.0:
    result = @[]
    for db in response["databases"].items():
      result.add(db["name"].toString())
  else:
    raise new(Exception)

proc createCollection*(db: Database[Mongo], name: string, capped: bool = false, autoIndexId: bool = true, maxSize: int = 0, maxDocs: int = 0): StatusReply =
  ## Create collection inside database via sync connection
  var request = %*{"create": name}

  if capped: request["capped"] = capped
  if autoIndexId: request["autoIndexId"] = true
  if maxSize > 0: request["size"] = maxSize
  if maxSize > 0: request["max"] = maxDocs

  let response = db["$cmd"].makeQuery(request).one()
  return StatusReply(
    ok: response["ok"] == 1.0'f64
  )

proc createCollection*(db: Database[AsyncMongo], name: string, capped: bool = false, autoIndexId: bool = true, maxSize: int = 0, maxDocs: int = 0): Future[StatusReply] {.async.} =
  ## Create collection inside database via async connection
  var request = %*{"create": name}

  if capped: request["capped"] = capped
  if autoIndexId: request["autoIndexId"] = true
  if maxSize > 0: request["size"] = maxSize
  if maxSize > 0: request["max"] = maxDocs

  let response = await db["$cmd"].makeQuery(request).one()
  return StatusReply(
    ok: response["ok"] == 1.0'f64
  )

proc listCollections*(db: Database[Mongo], filter: Bson = %*{}): seq[string] =
  ## List collections inside specified database
  let response = db["$cmd"].makeQuery(%*{"listCollections": 1'i32}).one()
  result = @[]
  if response["ok"] == 1.0:
    for col in response["cursor"]["firstBatch"]:
      result.add(col["name"])

proc listCollections*(db: Database[AsyncMongo], filter: Bson = %*{}): Future[seq[string]] {.async.} =
  ## List collections inside specified database via async connection
  let
    request = %*{"listCollections": 1'i32}
    response = await db["$cmd"].makeQuery(request).one()
  result = @[]
  if response["ok"] == 1.0'f64:
    for col in response["cursor"]["firstBatch"]:
      result.add(col["name"])

proc rename*(c: Collection[Mongo], newName: string, dropTarget: bool = false): StatusReply =
  ## Rename collection
  let
    request = %*{
      "renameCollection": $c,
      "to": "$#.$#" % [c.db.name, newName],
      "dropTarget": dropTarget
    }
    response = c.db.client["admin"]["$cmd"].makeQuery(request).one()
  c.name = newName
  return StatusReply(
    ok: response["ok"] == 1.0'f64
  )

proc rename*(c: Collection[AsyncMongo], newName: string, dropTarget: bool = false): Future[StatusReply] {.async.} =
  ## Rename collection via async connection
  let
    request = %*{
      "renameCollection": $c,
      "to": "$#.$#" % [c.db.name, newName],
      "dropTarget": dropTarget
    }
    response = await c.db.client["admin"]["$cmd"].makeQuery(request).one()
  c.name = newName
  return StatusReply(
    ok: response["ok"] == 1.0'f64
  )

proc drop*(db: Database[Mongo]): bool =
  ## Drop database from server
  let response = db["$cmd"].makeQuery(%*{"dropDatabase": 1}).one()
  return response["ok"] == 1.0

proc drop*(db: Database[AsyncMongo]): Future[bool] {.async.} =
  ## Drop database from server via async connection
  let response = await db["$cmd"].makeQuery(%*{"dropDatabase": 1}).one()
  return response["ok"] == 1.0

proc drop*(c: Collection[Mongo]): tuple[ok: bool, message: string] =
  ## Drop collection from database
  let response = c.db["$cmd"].makeQuery(%*{"drop": c.name}).one()
  let ok = response["ok"] == 1.0
  return (ok: ok, message: if ok: "" else: response["errmsg"])

proc drop*(c: Collection[AsyncMongo]): Future[tuple[ok: bool, message: string]] {.async.} =
  ## Drop collection from database via async clinet
  let response = await c.db["$cmd"].makeQuery(%*{"drop": c.name}).one()
  let ok = response["ok"] == 1.0
  return (ok: ok, message: if ok: "" else: response["errmsg"])

proc count*(c: Collection[Mongo]): int =
  ## Return number of documents in collection
  let x = c.db["$cmd"].makeQuery(%*{"count": c.name}).one()["n"]
  if x.kind == BsonKindInt32:
    return x.toInt32()
  elif x.kind == BsonKindDouble:
    return x.toFloat64.int

proc count*(c: Collection[AsyncMongo]): Future[int] {.async.} =
  ## Return number of documents in collection via async client
  let
    res = await c.db["$cmd"].makeQuery(%*{"count": c.name}).one()
    x = res["n"]
  if x.kind == BsonKindInt32:
    return x.toInt32()
  elif x.kind == BsonKindDouble:
    return x.toFloat64.int

proc count*(f: Cursor[Mongo]): int =
  ## Return number of documents in find query result
  let x = f.collection.db["$cmd"].makeQuery(%*{"count": f.collection.name, "query": f.query["$query"]}).one()["n"]
  if x.kind == BsonKindInt32:
    return x.toInt32()
  elif x.kind == BsonKindDouble:
    return x.toFloat64().int

proc count*(f: Cursor[AsyncMongo]): Future[int] {.async.} =
  ## Return number of document in find query result via async connection
  let
    response = await f.collection.db["$cmd"].makeQuery(%*{
      "count": f.collection.name,
      "query": f.query["$query"]
    }).one()
    x = response["n"]
  if x.kind == BsonKindInt32:
    return x.toInt32()
  elif x.kind == BsonKindDouble:
    return x.toFloat64().int

proc sort*[T:Mongo|AsyncMongo](f: Cursor[T], criteria: Bson): Cursor[T] =
  ## Setup sorting criteria
  f.sorting = criteria
  return f

proc unique*(f: Cursor[Mongo], key: string): seq[string] =
  ## Force cursor to return only distinct documents by specified field.
  ## Corresponds to '.distinct()' MongoDB command. If Nim we use 'unique'
  ## because 'distinct' is Nim's reserved keyword.
  let
    request = %*{
      "distinct": f.collection.name,
      "query": f.query["$query"],
      "key": key
    }
    response = f.collection.db["$cmd"].makeQuery(request).one()

  result = @[]
  if response["ok"] == 1.0'f64:
    for item in response["values"].items():
      result.add(item.toString())

proc unique*(f: Cursor[AsyncMongo], key: string): Future[seq[string]] {.async.} =
  ## Force cursor to return only distinct documents by specified field.
  ## Corresponds to '.distinct()' MongoDB command. If Nim we use 'unique'
  ## because 'distinct' is Nim's reserved keyword.
  let
    request = %*{
      "distinct": f.collection.name,
      "query": f.query["$query"],
      "key": key
    }
    response = await f.collection.db["$cmd"].makeQuery(request).one()

  result = @[]
  if response["ok"] == 1.0'f64:
    for item in response["values"].items():
      result.add(item.toString())

proc getLastError*(m: Mongo): StatusReply =
  ## Get last error happened in current connection
  let response = m["admin"]["$cmd"].makeQuery(%*{"getLastError": 1'i32}).one()
  return StatusReply(
    ok:  response["ok"] == 1.0'f64,
    n:   response["n"].toInt32(),
    err: if response["err"].kind == BsonKindStringUTF8: response["err"] else: ""
  )

proc getLastError*(am: AsyncMongo): Future[StatusReply] {.async.} =
  ## Get last error happened in current connection
  let response = await am["admin"]["$cmd"].makeQuery(%*{"getLastError": 1'i32}).one()
  return StatusReply(
    ok:  response["ok"] == 1.0'f64,
    n:   response["n"].toInt32(),
    err: if response["err"].kind == BsonKindStringUTF8: response["err"] else: ""
  )

# ============= #
# Insert API    #
# ============= #

proc insert*(c: Collection[Mongo], documents: seq[Bson], ordered: bool = true, writeConcern: Bson = nil): StatusReply {.discardable.} =
  ## Insert several new documents into MongoDB using one request
  let
    request = %*{
      "insert": c.name,
      "documents": documents,
      "ordered": ordered,
      "writeConcern": if writeConcern == nil: c.client.writeConcern else: writeConcern
    }
    response = c.db["$cmd"].makeQuery(request).one()

  return StatusReply(
    ok: response["ok"] == 1'i32,
    n: response["n"].toInt32()
  )

proc insert*(c: Collection[Mongo], document: Bson, ordered: bool = true, writeConcern: Bson = nil): StatusReply {.discardable.} =
  ## Insert new document into MongoDB via sync connection
  return c.insert(@[document], ordered, if writeConcern == nil: c.client.writeConcern else: writeConcern)

proc insert*(c: Collection[AsyncMongo], documents: seq[Bson], ordered: bool = true, writeConcern: Bson = nil): Future[StatusReply] {.async.} =
  ## Insert new documents into MongoDB via async connection
  let
    request = %*{
      "insert": c.name,
      "documents": documents,
      "ordered": ordered,
      "writeConcern": if writeConcern == nil: c.client.writeConcern else: writeConcern
    }
    response = await c.db["$cmd"].makeQuery(request).one()

  return StatusReply(
    ok: response["ok"] == 1'i32,
    n: response["n"].toInt32()
  )

proc insert*(c: Collection[AsyncMongo], document: Bson, ordered: bool = true, writeConcern: Bson = nil): Future[StatusReply] {.async.} =
  ## Insert new document into MongoDB via async connection
  result = await c.insert(@[document], ordered, if writeConcern == nil: c.client.writeConcern else: writeConcern)

# =========== #
# Update API  #
# =========== #

proc update*(c: Collection[Mongo], selector: Bson, update: Bson, multi: bool, upsert: bool): StatusReply {.discardable.} =
  ## Update MongoDB document[s]
  let
    request = %*{
      "update": c.name,
      "updates": [%*{"q": selector, "u": update, "upsert": upsert, "multi": multi}],
      "ordered": true
    }
    response = c.db["$cmd"].makeQuery(request).one()
  return StatusReply(
    ok: response["ok"] == 1'i32,
    n: response["n"].toInt32()
  )

proc update*(c: Collection[AsyncMongo], selector: Bson, update: Bson, multi: bool, upsert: bool): Future[bool] {.async.} =
  ## Update MongoDB document[s] via async connection
  let request = %*{
    "update": c.name,
    "updates": [%*{"q": selector, "u": update, "upsert": upsert, "multi": multi}],
    "ordered": true
  }
  let response = await c.db["$cmd"].makeQuery(request).one()
  return StatusReply(
    ok: response["ok"] == 1'i32,
    n: response["n"].toInt32()
  )

# ============ #
# Remove API   #
# ============ #

proc remove*(c: Collection[Mongo], selector: Bson, limit: int = 0, ordered: bool = true, writeConcern: Bson = nil): StatusReply {.discardable.} =
  ## Delete document[s] from MongoDB
  let
    request = %*{
      "delete": c.name,
      "deletes": [%*{"q": selector, "limit": limit}],
      "ordered": true,
      "writeConcern": if writeConcern == nil: c.client.writeConcern else: writeConcern
    }
    response = c.db["$cmd"].makeQuery(request).one()
  return StatusReply(
    ok: response["ok"] == 1'i32,
    n:  response["n"].toInt32(),
  )

proc remove*(c: Collection[AsyncMongo], selector: Bson, limit: int = 0, ordered: bool = true, writeConcern: Bson = nil): Future[StatusReply] {.async.} =
  ## Delete document[s] from MongoDB via asyn connection
  let
    request = %*{
      "delete": c.name,
      "deletes": [%*{"q": selector, "limit": limit}],
      "ordered": true,
      "writeConcern": if writeConcern == nil: c.client.writeConcern else: writeConcern
    }
    response = await c.db["$cmd"].makeQuery(request).one()
  return StatusReply(
    ok: response["ok"] == 1'i32,
    n:  response["n"].toInt32(),
  )

# =============== #
# User management
# =============== #

proc createUser*(db: DataBase[Mongo], username: string, pwd: string, customData: Bson = initBsonDocument(), roles: Bson = initBsonArray()): bool =
  ## Create new user for the specified database
  let createUserRequest = %*{
    "createUser": username,
    "pwd": pwd,
    "customData": customData,
    "roles": roles,
    "writeConcern": db.client.writeConcern
  }
  let response = db["$cmd"].makeQuery(createUserRequest).one()
  return StatusReply(
    ok: response["ok"] == 1.0'f64,
    n: 0
  )

proc createUser*(db: Database[AsyncMongo], username: string, pwd: string, customData: Bson = initBsonDocument(), roles: Bson = initBsonArray()): Future[bool] {.async.} =
  ## Create new user for the specified database via async client
  let
    createUserRequest = %*{
      "createUser": username,
      "pwd": pwd,
      "customData": customData,
      "roles": roles,
      "writeConcern": db.client.writeConcern
    }
    response = await db["$cmd"].makeQuery(createUserRequest).one()
  return StatusReply(
    ok: response["ok"] == 1.0'f64,
    n: 0
  )

proc dropUser*(db: Database[Mongo], username: string): bool =
  ## Drop user from the db
  let
    dropUserRequest = %*{
      "dropUser": username,
      "writeConcern": db.client.writeConcern
      }
    response = db["$cmd"].makeQuery(dropUserRequest).one()
  return StatusReply(
    ok: response["ok"] == 1.0'f64,
    n: 0
  )

proc dropUser*(db: Database[AsyncMongo], username: string): Future[bool] {.async.} =
  ## Drop user from the db via async client
  let
    dropUserRequest = %*{
      "dropUser": username,
      "writeConcern": db.client.writeConcern
    }
    response = await db["$cmd"].makeQuery(dropUserRequest).one()
  return StatusReply(
    ok: response["ok"] == 1.0'f64,
    n: 0
  )

# ============== #
# Authentication #
# ============== #

proc authenticateScramSha1(db: Database[Mongo], username: string, password: string): bool {.discardable.} =
  ## Authenticate connection (sync): using SCRAM-SHA-1 auth method
  if username == "" or password == "":
    return false

  let uname = username.replace("=", "=3D").replace(",", "=2C")
  let rand = random.random(1.0)
  let nonce = base64.encode(($rand)[2..^1])
  let fb = "n=" & uname & ",r=" & nonce

  let requestStart = %*{
    "saslStart": 1'i32,
    "mechanism": "SCRAM-SHA-1",
    "payload": bin("n,," & fb),
    "autoAuthorize": 1'i32
  }
  let responseStart = db["$cmd"].makeQuery(requestStart).one()
  ## line to check if connect worked
  if isNil(responseStart) or not isNil(responseStart["code"]): return false #connect failed or auth failure
  db.client.authenticated = true
  let responsePayload = binstr(responseStart["payload"])

  proc parsePayload(p: string): Table[string, string] =
    result = initTable[string, string]()
    for item in p.split(","):
      let e = item.find('=')
      let key = item[0..e - 1]
      let val = item[e + 1..^1]
      result[key] = val

  var parsedPayload = parsePayload(responsePayload)
  let iterations = parseInt(parsedPayload["i"])
  let salt = base64.decode(parsedPayload["s"])
  let rnonce = parsedPayload["r"]

  let withoutProof = "c=biws,r=" & rnonce
  let passwordDigest = $toMd5("$#:mongo:$#" % [username, password])
  var saltedPass = pbkdf2_hmac_sha1(20, passwordDigest, salt, iterations.uint32)

  proc stringWithSHA1Digest(d: SHA1Digest): string =
      result = newString(d.len)
      copyMem(addr result[0], unsafeAddr d[0], d.len)

  let client_key = Sha1Digest(hmac_sha1(saltedPass, "Client Key"))
  let stored_key = stringWithSHA1Digest(sha1.compute(client_key))

  let auth_msg = join([fb, responsePayload, withoutProof], ",")

  let client_sig = Sha1Digest(hmac_sha1(stored_key, auth_msg))

  var toEncode = newString(20)
  for i in 0..<client_key.len():
    toEncode[i] = cast[char](cast[uint8](client_key[i]) xor cast[uint8](client_sig[i]))

  let client_proof = "p=" & base64.encode(toEncode)
  let client_final = join([without_proof, client_proof], ",")

  let requestContinue1 = %*{
    "saslContinue": 1'i32,
    "conversationId": toInt32(responseStart["conversationId"]),
    "payload": bin(client_final)
  }
  let responseContinue1 =  db["$cmd"].makeQuery(requestContinue1).one()

  let server_key = stringWithSHA1Digest(Sha1Digest(hmac_sha1(salted_pass, "Server Key")))
  let server_sig = base64.encode(stringWithSHA1Digest(Sha1Digest(hmac_sha1(server_key, auth_msg))))

  parsedPayload = parsePayload(binstr(responseContinue1["payload"]))

  proc compare_digest(a, b: string): bool =
    var res : uint8
    for i in 0 ..< a.len:
      res = res or (cast[uint8](a[i]) xor cast[uint8](b[i]))
    result = res == 0

  if not compare_digest(parsedPayload["v"], server_sig):
      raise newException(Exception, "Server returned an invalid signature.")

  # Depending on how it's configured, Cyrus SASL (which the server uses)
  # requires a third empty challenge.
  if not responseContinue1["done"].toBool():
      let requestContinue2 = %*{
        "saslContinue": 1'i32,
        "conversationId": responseContinue1["conversationId"],
        "payload": ""
      }
      let responseContinue2 = db["$cmd"].makeQuery(requestContinue2).one()
      if not responseContinue2["done"].toBool():
          raise newException(Exception, "SASL conversation failed to complete.")
  return true

proc authenticateScramSha1(db: Database[AsyncMongo], username: string, password: string): Future[bool] {.async.} =
  ## Authenticate connection (async) using SCRAM-SHA-1 method
  if username == "" or password == "":
    return false

  let uname = username.replace("=", "=3D").replace(",", "=2C")
  let rand = random.random(1.0)
  let nonce = base64.encode(($rand)[2..^1])
  let fb = "n=" & uname & ",r=" & nonce

  let requestStart = %*{
    "saslStart": 1'i32,
    "mechanism": "SCRAM-SHA-1",
    "payload": bin("n,," & fb),
    "autoAuthorize": 1'i32
  }
  let responseStart = await db["$cmd"].makeQuery(requestStart).one()
  let responsePayload = binstr(responseStart["payload"])

  proc parsePayload(p: string): Table[string, string] =
    result = initTable[string, string]()
    for item in p.split(","):
      let e = item.find('=')
      let key = item[0..e - 1]
      let val = item[e + 1..^1]
      result[key] = val

  var parsedPayload = parsePayload(responsePayload)
  let iterations = parseInt(parsedPayload["i"])
  let salt = base64.decode(parsedPayload["s"])
  let rnonce = parsedPayload["r"]

  let withoutProof = "c=biws,r=" & rnonce
  let passwordDigest = $toMd5("$#:mongo:$#" % [username, password])
  var saltedPass = pbkdf2_hmac_sha1(20, passwordDigest, salt, iterations.uint32)

  proc stringWithSHA1Digest(d: SHA1Digest): string =
    result = newString(d.len)
    copyMem(addr result[0], unsafeAddr d[0], d.len)

  let client_key = Sha1Digest(hmac_sha1(saltedPass, "Client Key"))
  let stored_key = stringWithSHA1Digest(sha1.compute(client_key))

  let auth_msg = join([fb, responsePayload, withoutProof], ",")

  let client_sig = Sha1Digest(hmac_sha1(stored_key, auth_msg))

  var toEncode = newString(20)
  for i in 0..<client_key.len():
    toEncode[i] = cast[char](cast[uint8](client_key[i]) xor cast[uint8](client_sig[i]))

  let client_proof = "p=" & base64.encode(toEncode)
  let client_final = join([without_proof, client_proof], ",")

  let requestContinue1 = %*{
    "saslContinue": 1'i32,
    "conversationId": toInt32(responseStart["conversationId"]),
    "payload": bin(client_final)
  }
  let responseContinue1 = await db["$cmd"].makeQuery(requestContinue1).one()

  let server_key = stringWithSHA1Digest(Sha1Digest(hmac_sha1(salted_pass, "Server Key")))
  let server_sig = base64.encode(stringWithSHA1Digest(Sha1Digest(hmac_sha1(server_key, auth_msg))))

  parsedPayload = parsePayload(binstr(responseContinue1["payload"]))

  proc compare_digest(a, b: string): bool =
    var res : uint8
    for i in 0 ..< a.len:
      res = res or (cast[uint8](a[i]) xor cast[uint8](b[i]))
    result = res == 0

  if not compare_digest(parsedPayload["v"], server_sig):
    raise newException(Exception, "Server returned an invalid signature.")

  # Depending on how it's configured, Cyrus SASL (which the server uses)
  # requires a third empty challenge.
  if not responseContinue1["done"].toBool():
    let requestContinue2 = %*{
      "saslContinue": 1'i32,
      "conversationId": responseContinue1["conversationId"],
      "payload": ""
    }
    let responseContinue2 = await db["$cmd"].makeQuery(requestContinue2).one()
    if not responseContinue2["done"].toBool():
      raise newException(Exception, "SASL conversation failed to complete.")
  return true

proc authenticate*(db: Database[Mongo], username: string, password: string): bool {.discardable.} =
  ## Authenticate connection (sync): using MONGODB-CR auth method
  if username == "" or password == "":
    return false

  let nonce: string = db["$cmd"].makeQuery(%*{"getnonce": 1'i32}).one()["nonce"]
  let passwordDigest = $toMd5("$#:mongo:$#" % [username, password])
  let key = $toMd5("$#$#$#" % [nonce, username, passwordDigest])
  let request = %*{
    "authenticate": 1'i32,
    "mechanism": "MONGODB-CR",
    "user": username,
    "nonce": nonce,
    "key": key,
    "autoAuthorize": 1'i32
  }
  let response = db["$cmd"].makeQuery(request).one()
  return if response["ok"] == 0: false else: true

proc connect*(m: Mongo): bool =
  ## Connect socket to mongo server
  try:
      m.sock.connect(m.host, net.Port(m.port), -1)
  except OSError:
      return false
  return true

proc newMongoDatabase*(u: Uri): Database[Mongo] =
  ## Create new Mongo sync client using URI type
  let client = newMongoWithURI(u)
  if client.connect():
    result.new()
    result.name = u.path.extractFileName()
    result.client = client
    result.client.authenticated = result.authenticateScramSha1(client.username, client.password)

proc newMongoDatabase*(u: string): Database[Mongo] =
  ## Create new Mongo sync client using URI as string
  return newMongoDatabase(parseUri(u))

proc newAsyncMongoDatabase*(u: Uri): Future[Database[AsyncMongo]] {.async.} =
  ## Create new AsyncMongo client using URI as string
  let client = newAsyncMongoWithURI(u)
  if await client.connect():
    result.new()
    result.name = u.path.extractFileName()
    result.client = client

proc newAsyncMongoDatabase*(u: string): Future[Database[AsyncMongo]] = newAsyncMongoDatabase(parseUri(u))
