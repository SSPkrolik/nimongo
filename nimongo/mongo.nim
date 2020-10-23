import asyncdispatch
import asyncnet
import base64
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
import asyncfile
import strformat
import mimetypes

import bson except `()`

import scram/client

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

when compileOption("threads"):
  import locks

template lockIfThreads(body: untyped): untyped =
  when compileOption("threads"):
    {.locks: [f.collection.client.requestLock].}:
      body
  else:
    body

type
  Mongo* = ref object of MongoBase      ## Mongo client object
    when compileOption("threads"):
      requestLock:   Lock
    sock:          Socket
    authenticated*: bool

  AsyncLockedSocket = ref object
    inuse:         bool
    authenticated: bool
    connected:     bool
    sock:          AsyncSocket
    queue:         TableRef[int32, Future[seq[Bson]]]

  AsyncMongo* = ref object of MongoBase     ## Mongo async client object
    current:        int                     ## Current (possibly) free socket to use
    pool:           seq[AsyncLockedSocket]  ## Pool of connections
    authenticated*: bool                    ## If authenticated set to true

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

  GridFS*[T] = ref object of MongoBase
    ## GridFS is collection which namespaced to .files and .chunks
    name*: string    # bucket name
    files*: Collection[T]
    chunks*: Collection[T]


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
    sock:          newAsyncSocket(),
    queue:         newTable[int32, Future[seq[Bson]]]()
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

proc newAsyncMongoWithURI*(u: string, maxConnections=16): AsyncMongo = newAsyncMongoWithURI(parseUri(u), maxConnections)

proc next(am: AsyncMongo): Future[AsyncLockedSocket] {.async.} =
  ## Retrieves next non-in-use async socket for request
  while true:
    for _ in 0..<am.pool.len():
      am.current = (am.current + 1) mod am.pool.len()
      let s = am.pool[am.current]
      if not s.inuse:
        s.inuse = true
        if not s.connected:
          try:
            await s.sock.connect(am.host, asyncdispatch.Port(am.port))
            s.connected = true
          except OSError:
            continue
        return s
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
  for ls in am.pool:
    try:
      await ls.sock.connect(am.host, asyncdispatch.Port(am.port))
      ls.connected = true
      result = true
    except OSError:
      continue

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

proc prepareQuery(f: Cursor, requestId: int32, numberToReturn: int32, numberToSkip: int32): string =
  ## Prepare query and request queries for making OP_QUERY
  var bfields: Bson = newBsonDocument()
  if f.fields.len() > 0:
      for field in f.fields.items():
          bfields[field] = 1'i32.toBson()
  let squery = f.query.bytes()
  let sfields: string = if f.fields.len() > 0: bfields.bytes() else: ""

  result = ""
  let colName = $f.collection
  buildMessageHeader(int32(29 + colName.len + squery.len + sfields.len),
    requestId, 0, result)

  buildMessageQuery(0, colName, numberToSkip , numberToReturn, result)
  result &= squery
  result &= sfields

iterator performFind(f: Cursor[Mongo], numberToReturn: int32, numberToSkip: int32): Bson {.closure.} =
  ## Private procedure for performing actual query to Mongo
  lockIfThreads:
    if f.collection.client.sock.trySend(prepareQuery(f, f.collection.client.nextRequestId(), numberToReturn, numberToSkip)):
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
          yield newBsonDocument(stream)
      elif numberToReturn == 1:
        raise newException(NotFound, "No documents matching query were found")
      else:
        discard

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
    discard stream.readInt32()                     ## responseFlags
    discard stream.readInt64()                     ## cursorID
    discard stream.readInt32()                     ## startingFrom
    let numberReturned: int32 = stream.readInt32() ## numberReturned

    var res: seq[Bson] = @[]

    if numberReturned > 0:
      for i in 0..<numberReturned:
        res.add(newBsonDocument(stream))

    let fut = ls.queue[responseTo]
    ls.queue.del responseTo
    fut.complete(res)


proc performFindAsync(f: Cursor[AsyncMongo], numberToReturn, numberToSkip: int32, lockedSocket: AsyncLockedSocket = nil): Future[seq[Bson]] {.async.} =
  ## Perform asynchronous OP_QUERY operation to MongoDB.

  ## Private procedure for performing actual query to Mongo via async client
  var ls = lockedSocket
  if ls.isNil:
    ls = await f.collection.client.next()

  let requestId = f.collection.client.nextRequestId()
  await ls.sock.send(prepareQuery(f, requestId, numberToReturn, numberToSkip))
  ls.inuse = false
  let response = newFuture[seq[Bson]]("recv")
  ls.queue[requestId] = response
  if ls.queue.len == 1:
    asyncCheck handleResponses(ls)
  result = await response

proc all*(f: Cursor[Mongo]): seq[Bson] =
  ## Perform MongoDB query and return all matching documents
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
  if docs.len == 0:
    raise newException(NotFound, "No documents matching query were found")
  return docs[0]

proc oneOrNone*(f: Cursor[AsyncMongo]): Future[Bson] {.async.} =
  ## Perform MongoDB query asynchronously and return first matching document or
  ## nil if not found.
  let docs = await f.performFindAsync(1, f.nskip)
  if docs.len > 0:
    result = docs[0]

proc one(f: Cursor[AsyncMongo], ls: AsyncLockedSocket): Future[Bson] {.async.} =
  # Internal proc used for sending authentication requests on particular socket
  let docs = await f.performFindAsync(1, f.nskip, ls)
  if docs.len == 0:
    raise newException(NotFound, "No documents matching query were found")
  return docs[0]

iterator items*(f: Cursor): Bson =
  ## Perform MongoDB query and return iterator for all matching documents
  for doc in f.performFind(f.nlimit, f.nskip):
      yield doc

iterator itemsForceSync*(f: Cursor[AsyncMongo]): Bson =
  var count = 0'i32
  var limit = f.nlimit
  if limit == 0: # pending https://github.com/SSPkrolik/nimongo/issues/64
    limit = type(f.nlimit).high
  while count < limit:
    let docs = waitFor f.performFindAsync(1, f.nskip + count)
    if docs.len == 0:
      break
    count.inc
    yield docs[0]

proc isMaster*(sm: Mongo): bool =
  ## Perform query in order to check if connected Mongo instance is a master
  return sm["admin"]["$cmd"].makeQuery(%*{"isMaster": 1}).one()["ismaster"].toBool

proc isMaster*(am: AsyncMongo): Future[bool] {.async.} =
  ## Perform query in order to check if ocnnected Mongo instance is a master
  ## via async connection.
  let response = await am["admin"]["$cmd"].makeQuery(%*{"isMaster": 1}).one()
  return response["ismaster"].toBool

proc listDatabases*(sm: Mongo): seq[string] =
  ## Return list of databases on the server
  let response = sm["admin"]["$cmd"].makeQuery(%*{"listDatabases": 1}).one()
  if response.isReplyOk:
    for db in response["databases"].items():
      result.add(db["name"].toString())

proc listDatabases*(am: AsyncMongo): Future[seq[string]] {.async.} =
  ## Return list of databases on the server via async client
  let response = await am["admin"]["$cmd"].makeQuery(%*{"listDatabases": 1}).one()
  if response.isReplyOk:
    for db in response["databases"].items():
      result.add(db["name"].toString())

proc createCollection*(db: Database[Mongo], name: string, capped: bool = false, autoIndexId: bool = true, maxSize: int = 0, maxDocs: int = 0): StatusReply =
  ## Create collection inside database via sync connection
  var request = %*{"create": name}

  if capped: request["capped"] = capped.toBson()
  if autoIndexId: request["autoIndexId"] = true.toBson()
  if maxSize > 0: request["size"] = maxSize.toBson()
  if maxSize > 0: request["max"] = maxDocs.toBson()

  let response = db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

proc createCollection*(db: Database[AsyncMongo], name: string, capped: bool = false, autoIndexId: bool = true, maxSize: int = 0, maxDocs: int = 0): Future[StatusReply] {.async.} =
  ## Create collection inside database via async connection
  var request = %*{"create": name}

  if capped: request["capped"] = capped.toBson()
  if autoIndexId: request["autoIndexId"] = true.toBson()
  if maxSize > 0: request["size"] = maxSize.toBson()
  if maxSize > 0: request["max"] = maxDocs.toBson()

  let response = await db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

proc listCollections*(db: Database[Mongo], filter: Bson = %*{}): seq[string] =
  ## List collections inside specified database
  let response = db["$cmd"].makeQuery(%*{"listCollections": 1'i32}).one()
  if response.isReplyOk:
    for col in response["cursor"]["firstBatch"]:
      result.add(col["name"].toString)

proc listCollections*(db: Database[AsyncMongo], filter: Bson = %*{}): Future[seq[string]] {.async.} =
  ## List collections inside specified database via async connection
  let
    request = %*{"listCollections": 1'i32}
    response = await db["$cmd"].makeQuery(request).one()
  if response.isReplyOk:
    for col in response["cursor"]["firstBatch"]:
      result.add(col["name"].toString)

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
  return response.toStatusReply

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
  return response.toStatusReply

proc drop*(db: Database[Mongo]): bool =
  ## Drop database from server
  let response = db["$cmd"].makeQuery(%*{"dropDatabase": 1}).one()
  return response.isReplyOk

proc drop*(db: Database[AsyncMongo]): Future[bool] {.async.} =
  ## Drop database from server via async connection
  let response = await db["$cmd"].makeQuery(%*{"dropDatabase": 1}).one()
  return response.isReplyOk

proc drop*(c: Collection[Mongo]): tuple[ok: bool, message: string] =
  ## Drop collection from database
  let response = c.db["$cmd"].makeQuery(%*{"drop": c.name}).one()
  let status = response.toStatusReply
  return (ok: status.ok, message: status.err)

proc drop*(c: Collection[AsyncMongo]): Future[tuple[ok: bool, message: string]] {.async.} =
  ## Drop collection from database via async clinet
  let response = await c.db["$cmd"].makeQuery(%*{"drop": c.name}).one()
  let status = response.toStatusReply
  return (ok: status.ok, message: status.err)

proc stats*(c: Collection[Mongo]): Bson =
  return c.db["$cmd"].makeQuery(%*{"collStats": c.name}).one()

proc stats*(c: Collection[AsyncMongo]): Future[Bson] {.async.} =
  return await c.db["$cmd"].makeQuery(%*{"collStats": c.name}).one()

proc count*(c: Collection[Mongo]): int =
  ## Return number of documents in collection
  return c.db["$cmd"].makeQuery(%*{"count": c.name}).one().getReplyN

proc count*(c: Collection[AsyncMongo]): Future[int] {.async.} =
  ## Return number of documents in collection via async client
  return (await c.db["$cmd"].makeQuery(%*{"count": c.name}).one()).getReplyN

proc count*(f: Cursor[Mongo]): int =
  ## Return number of documents in find query result
  return f.collection.db["$cmd"].makeQuery(%*{"count": f.collection.name, "query": f.query["$query"]}).one().getReplyN

proc count*(f: Cursor[AsyncMongo]): Future[int] {.async.} =
  ## Return number of document in find query result via async connection
  let
    response = await f.collection.db["$cmd"].makeQuery(%*{
      "count": f.collection.name,
      "query": f.query["$query"]
    }).one()
  return response.getReplyN

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

  if response.isReplyOk:
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

  if response.isReplyOk:
    for item in response["values"].items():
      result.add(item.toString())

proc getLastError*(m: Mongo): StatusReply =
  ## Get last error happened in current connection
  let response = m["admin"]["$cmd"].makeQuery(%*{"getLastError": 1'i32}).one()
  return response.toStatusReply

proc getLastError*(am: AsyncMongo): Future[StatusReply] {.async.} =
  ## Get last error happened in current connection
  let response = await am["admin"]["$cmd"].makeQuery(%*{"getLastError": 1'i32}).one()
  return response.toStatusReply

# ============= #
# Insert API    #
# ============= #

proc insert*(c: Collection[Mongo], documents: seq[Bson], ordered: bool = true, writeConcern: Bson = nil): StatusReply {.discardable.} =
  ## Insert several new documents into MongoDB using one request

  # 
  # insert any missing _id fields
  #
  var inserted_ids: seq[Bson] = @[]
  var id = ""
  for doc in documents:
    if not doc.contains("_id"):
      doc["_id"] = toBson(genOid())
    inserted_ids.add(doc["_id"])

  #
  # build & send Mongo query
  #
  let
    request = %*{
      "insert": c.name,
      "documents": documents,
      "ordered": ordered,
      "writeConcern": if writeConcern == nil.Bson: c.client.writeConcern else: writeConcern
    }
    response = c.db["$cmd"].makeQuery(request).one()

  return response.toStatusReply(inserted_ids=inserted_ids)

proc insert*(c: Collection[Mongo], document: Bson, ordered: bool = true, writeConcern: Bson = nil): StatusReply {.discardable.} =
  ## Insert new document into MongoDB via sync connection
  return c.insert(@[document], ordered, if writeConcern == nil.Bson: c.client.writeConcern else: writeConcern)

proc insert*(c: Collection[AsyncMongo], documents: seq[Bson], ordered: bool = true, writeConcern: Bson = nil): Future[StatusReply] {.async.} =
  ## Insert new documents into MongoDB via async connection

  # 
  # insert any missing _id fields
  #
  var inserted_ids: seq[Bson] = @[]
  var id = ""
  for doc in documents:
    if not doc.contains("_id"):
      doc["_id"] = toBson(genOid())
    inserted_ids.add(doc["_id"])

  #
  # build & send Mongo query
  #
  let
    request = %*{
      "insert": c.name,
      "documents": documents,
      "ordered": ordered,
      "writeConcern": if writeConcern == nil.Bson: c.client.writeConcern else: writeConcern
    }
    response = await c.db["$cmd"].makeQuery(request).one()

  return response.toStatusReply(inserted_ids=inserted_ids)

proc insert*(c: Collection[AsyncMongo], document: Bson, ordered: bool = true, writeConcern: Bson = nil): Future[StatusReply] {.async.} =
  ## Insert new document into MongoDB via async connection
  result = await c.insert(@[document], ordered, if writeConcern == nil.Bson: c.client.writeConcern else: writeConcern)

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
  return response.toStatusReply

proc update*(c: Collection[AsyncMongo], selector: Bson, update: Bson, multi: bool, upsert: bool): Future[StatusReply] {.async.} =
  ## Update MongoDB document[s] via async connection
  let request = %*{
    "update": c.name,
    "updates": [%*{"q": selector, "u": update, "upsert": upsert, "multi": multi}],
    "ordered": true
  }
  let response = await c.db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

# ==================== #
# Find and modify API  #
# ==================== #

proc findAndModify*(c: Collection[Mongo], selector: Bson, sort: Bson, update: Bson, afterUpdate: bool, upsert: bool, writeConcern: Bson = nil, remove: bool = false): Future[StatusReply] {.async.} =
  ## Finds and modifies MongoDB document
  let request = %*{
    "findAndModify": c.name,
    "query": selector,
    "new": afterUpdate,
    "upsert": upsert,
    "writeConcern": if writeConcern == nil.Bson: c.client.writeConcern else: writeConcern
  }
  if not sort.isNil:
    request["sort"] = sort
  if remove:
    request["remove"] = remove.toBson()
  else:
    request["update"] = update
  let response = c.db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

proc findAndModify*(c: Collection[AsyncMongo], selector: Bson, sort: Bson, update: Bson, afterUpdate: bool, upsert: bool, writeConcern: Bson = nil, remove: bool = false): Future[StatusReply] {.async.} =
  ## Finds and modifies MongoDB document via async connection
  let request = %*{
    "findAndModify": c.name,
    "query": selector,
    "new": afterUpdate,
    "upsert": upsert,
    "writeConcern": if writeConcern == nil.Bson: c.client.writeConcern else: writeConcern
  }
  if not sort.isNil:
    request["sort"] = sort
  if remove:
    request["remove"] = remove.toBson()
  else:
    request["update"] = update
  let response = await c.db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

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
      "writeConcern": if writeConcern == nil.Bson: c.client.writeConcern else: writeConcern
    }
    response = c.db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

proc remove*(c: Collection[AsyncMongo], selector: Bson, limit: int = 0, ordered: bool = true, writeConcern: Bson = nil): Future[StatusReply] {.async.} =
  ## Delete document[s] from MongoDB via asyn connection
  let
    request = %*{
      "delete": c.name,
      "deletes": [%*{"q": selector, "limit": limit}],
      "ordered": true,
      "writeConcern": if writeConcern == nil.Bson: c.client.writeConcern else: writeConcern
    }
    response = await c.db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

# =============== #
# User management
# =============== #

proc createUser*(db: DataBase[Mongo], username: string, pwd: string, customData: Bson = newBsonDocument(), roles: Bson = newBsonArray()): bool =
  ## Create new user for the specified database
  let createUserRequest = %*{
    "createUser": username,
    "pwd": pwd,
    "customData": customData,
    "roles": roles,
    "writeConcern": db.client.writeConcern
  }
  let response = db["$cmd"].makeQuery(createUserRequest).one()
  return response.isReplyOk

proc createUser*(db: Database[AsyncMongo], username: string, pwd: string, customData: Bson = newBsonDocument(), roles: Bson = newBsonArray()): Future[bool] {.async.} =
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
  return response.isReplyOk

proc dropUser*(db: Database[Mongo], username: string): bool =
  ## Drop user from the db
  let
    dropUserRequest = %*{
      "dropUser": username,
      "writeConcern": db.client.writeConcern
      }
    response = db["$cmd"].makeQuery(dropUserRequest).one()
  return response.isReplyOk

proc dropUser*(db: Database[AsyncMongo], username: string): Future[bool] {.async.} =
  ## Drop user from the db via async client
  let
    dropUserRequest = %*{
      "dropUser": username,
      "writeConcern": db.client.writeConcern
    }
    response = await db["$cmd"].makeQuery(dropUserRequest).one()
  return response.isReplyOk

# ============== #
# Authentication #
# ============== #

proc authenticateScramSha1(db: Database[Mongo], username: string, password: string): bool {.discardable.} =
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
  let responseStart = db["$cmd"].makeQuery(requestStart).one()
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
  let responseContinue1 =  db["$cmd"].makeQuery(requestContinue1).one()

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
      let responseContinue2 = db["$cmd"].makeQuery(requestContinue2).one()
      if not responseContinue2["done"].toBool():
          raise newException(Exception, "SASL conversation failed to complete.")
  return true

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

proc authenticate*(db: Database[Mongo], username: string, password: string): bool {.discardable.} =
  ## Authenticate connection (sync): using MONGODB-CR auth method
  if username == "" or password == "":
    return false

  let nonce = db["$cmd"].makeQuery(%*{"getnonce": 1'i32}).one()["nonce"].toString
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
  return response.isReplyOk

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

proc newAsyncMongoDatabase*(u: Uri, maxConnections = 16): Future[Database[AsyncMongo]] {.async.} =
  ## Create new AsyncMongo client using URI as string
  let client = newAsyncMongoWithURI(u, maxConnections)
  if await client.connect():
    result.new()
    result.name = u.path.extractFileName()
    result.client = client

    var authenticated = newSeq[Future[bool]]()
    for s in client.pool:
      authenticated.add(result.authenticateScramSha1(client.username, client.password, s))

    let authRes = await all(authenticated)
    client.authenticated = authRes.any() do(x: bool) -> bool: x

proc newAsyncMongoDatabase*(u: string, maxConnections = 16): Future[Database[AsyncMongo]] = newAsyncMongoDatabase(parseUri(u), maxConnections)

# === GridFS API === #

proc createBucket*(db: Database[Mongo], name: string): GridFs[Mongo] =
  ## Create a grid-fs bucket collection name. Grid-fs actually just simply a collection consists of two
  ## 1. <bucket-name>.files
  ## 2. <nucket-name>.chunks
  ## Hence creating bucket is creating two collections at the same time.
  new result
  result.name = name
  let fcolname = name & ".files"
  let ccolname = name & ".chunks"
  let filecstat = db.createCollection(fcolname)
  let chunkcstat = db.createCollection(ccolname)
  if filecstat.ok and chunkcstat.ok:
    result.files = db[fcolname]
    result.chunks = db[ccolname]

proc createBucket*(db: Database[AsyncMongo], name: string): Future[GridFs[AsyncMongo]]{.async.} =
  ## Create a grid-fs bucket collection name async version
  new result
  result.name = name
  let fcolname = name & ".files"
  let ccolname = name & ".chunks"
  let collops = @[
    db.createCollection(fcolname),
    db.createCollection(ccolname)
  ]
  let statR = await all(collops)
  if statR.allIt( it.ok ):
    result.files = db[fcolname]
    result.chunks = db[ccolname]

proc getBucket*[T: Mongo|AsyncMongo](db: Database[T], name: string): GridFs[T] =
  ## Get the bucket (GridFS) instead of collection.
  let fcolname = name & ".files"
  let ccolname = name & ".chunks"
  new result
  result.files = db[fcolname]
  result.chunks = db[ccolname]
  result.name = name

proc `$`*(g: GridFS): string =
  #result = &"{g.files.db.name}.{g.name}"
  result = g.name

proc uploadFile*[T: Mongo|AsyncMongo](bucket: GridFs[T], f: AsyncFile, filename = "",
  metadata = null(), chunksize = 255 * 1024): Future[bool] {.async, discardable.} =
  ## Upload opened asyncfile with defined chunk size which defaulted at 255 KB
  let foid = genoid()
  let fsize = getFileSize f
  let fileentry = %*{
    "_id": foid,
    "chunkSize": chunkSize,
    "length": fsize,
    "uploadDate": now().toTime.timeUTC,
    "filename": filename,
    "metadata": metadata
  }
  when T is Mongo:
    let entrystatus = bucket.files.insert(fileentry)
  else:
    let entrystatus = await bucket.files.insert(fileentry)
  if not entrystatus.ok:
    echo &"cannot upload {filename}: {entrystatus.err}"
    return

  var chunkn = 0
  for _ in countup(0, int(fsize-1), chunksize):
    var chunk = %*{
      "files_id": foid,
      "n": chunkn
    }
    let data = await f.read(chunksize)
    chunk["data"] = bin data
    when T is Mongo:
      let chunkstatus = bucket.chunks.insert(chunk)
    else:
      let chunkstatus = await bucket.chunks.insesrt(chunk)
    if not chunkstatus.ok:
      echo &"problem happened when uploading: {chunkstatus.err}"
      return
    inc chunkn
  result = true

proc uploadFile*[T: Mongo|AsyncMongo](bucket: GridFS[T], filename: string,
  metadata = null(), chunksize = 255 * 1024): Future[bool] {.async, discardable.} =
  ## A higher uploadFile which directly open and close file from filename.
  var f: AsyncFile
  try:
    f = openAsync filename
  except IOError:
    echo getCurrentExceptionMsg()
    return
  defer: close f
  
  let (_, fname, ext) = splitFile filename
  let m = newMimeTypes()
  var filemetadata = metadata
  if filemetadata.kind != BsonKindNull and filemetadata.kind == BsonKindDocument:
    filemetadata["mime"] = m.getMimeType(ext).toBson
    filemetadata["ext"] = ext.toBson
  else:
    filemetadata = %*{
      "mime": m.getMimeType(ext),
      "exit": ext
    }
  result = await bucket.uploadFile(f, fname & ext,
    metadata = filemetadata, chunksize = chunksize)

proc downloadFile*[T: Mongo|AsyncMongo](bucket: GridFS[T], f: AsyncFile,
  filename = ""): Future[bool]
  {.async, discardable.} =
  ## Download given filename and write it to f asyncfile. This only download
  ## the latest uploaded file in the same name.
  let q = %*{ "filename": filename }
  let uploadDesc = %*{ "uploadDate": -1 }
  let fdata = bucket.files.find(q, @["_id", "length"]).orderBy(uploadDesc).one
  if fdata.isNil:
    echo &"cannot download {filename} to file: {getCurrentExceptionMsg()}"
    return

  let qchunk = %*{ "files_id": fdata["_id"] }
  let fsize = fdata["length"].toInt
  let selector = @["data"]
  let sort = %* { "n": 1 }
  var currsize = 0
  var skipdoc = 0
  while currsize < fsize:
    var chunks = bucket.chunks.find(qchunk, selector).skip(skipdoc.int32).orderBy(sort).all()
    skipdoc += chunks.len
    for chunk in chunks:
      let data = binstr chunk["data"]
      currsize += data.len
      await f.write(data)

  if currsize < fsize:
    echo &"incomplete file download; only at {currsize.float / fsize.float * 100}%"
    return

  result = true

proc downloadFile*[T: Mongo|AsyncMongo](bucket: GridFS[T], filename: string):
  Future[bool]{.async, discardable.} =
  ## Higher version for downloadFile. Ensure the destination file path has
  ## writing permission
  var f: AsyncFile
  try:
    f = openAsync(filename, fmWrite)
  except IOError:
    echo getCurrentExceptionMsg()
    return
  defer: close f
  let (dir, fname, ext) = splitFile filename
  result = await bucket.downloadFile(f,  fname & ext)
