import asyncdispatch
import asyncnet
import random
import md5
import net
import oids
import sequtils
import strutils
import tables
import typetraits
import times
import uri
import asyncfile
import strformat
import mimetypes
import os

import bson except `()`

import ./private/auth
import ./private/clientbase
import ./private/errors
import ./private/reply
import ./private/writeconcern
import ./private/async
when compileOption("threads"):
  import ./private/threaded as sync
else:
  import ./private/single as sync

randomize()

export auth
export clientbase except nextRequestId, init, calcReturnSize, updateCount, makeQuery, prepareMore, prepareQuery
export errors
export reply
export writeconcern
export async except acquire, release, refresh
export sync except acquire, release, refresh

# === Mongo client API === #

proc replica*[T:Mongo|AsyncMongo](mb: T, nodes: seq[tuple[host: string, port: uint16]]) =
  for node in nodes:
    when T is Mongo:
      mb.replicas.add((host: node.host, port: sockets.Port(node.port)))
    when T is AsyncMongo:
      mb.replicas.add((host: node.host, port: asyncnet.Port(node.port)))

# === Database API === #

proc newMongoDatabase*(u: string): Database[Mongo] {.deprecated.} =
  ## Create new Mongo sync client using URI as string
  return newMongoDatabase(parseUri(u))

proc newAsyncMongoDatabase*(u: string, maxConnections = 16): Future[Database[AsyncMongo]] {.deprecated.} = 
  ## Create new Mongo async client using URI as string
  return newAsyncMongoDatabase(parseUri(u), maxConnections)

# === Collection API === #

proc find*[T:Mongo|AsyncMongo](c: Collection[T], filter: Bson, fields: seq[string] = @[], maxTime: int32 = 0): Cursor[T] =
  ## Find query
  result = c.makeQuery(
    %*{
      "$query": filter
    },
    fields,
    maxTime
  )

# === Find API === #

proc all*(f: Cursor[Mongo]): seq[Bson] =
  ## Perform MongoDB query and return all matching documents
  while not f.isClosed():
    result.add(f.refresh())

proc all*(f: Cursor[AsyncMongo]): Future[seq[Bson]] {.async.} =
  ## Perform MongoDB query asynchronously and return all matching documents.
  while not f.isClosed():
    let ret = await f.refresh()
    result.add(ret)

proc one*(f: Cursor[Mongo]): Bson =
  ## Perform MongoDB query and return first matching document
  let docs = f.limit(1).refresh()
  if docs.len == 0:
    raise newException(NotFound, "No documents matching query were found")
  return docs[0]

proc one*(f: Cursor[AsyncMongo]): Future[Bson] {.async.} =
  ## Perform MongoDB query asynchronously and return first matching document.
  let docs = await f.limit(1).refresh()
  if docs.len == 0:
    raise newException(NotFound, "No documents matching query were found")
  return docs[0]

proc oneOrNone*(f: Cursor[Mongo]): Bson =
  ## Perform MongoDB query and return first matching document or
  ## nil if not found.
  let docs = f.limit(1).refresh()
  if docs.len > 0:
    result = docs[0]

proc oneOrNone*(f: Cursor[AsyncMongo]): Future[Bson] {.async.} =
  ## Perform MongoDB query asynchronously and return first matching document or
  ## nil if not found.
  let docs = await f.limit(1).refresh()
  if docs.len > 0:
    result = docs[0]

iterator items*(f: Cursor[Mongo]): Bson =
  ## Perform MongoDB query and return iterator for all matching documents
  while not f.isClosed():
    let docs = f.refresh()
    for doc in docs:
      yield doc

iterator items*(f: Cursor[AsyncMongo]): Bson =
  ## Perform MongoDB query and return iterator for all matching documents
  while not f.isClosed():
    let docs = waitFor f.refresh()
    for doc in docs:
      yield doc

iterator itemsForceSync*(f: Cursor[AsyncMongo]): Bson =
  while not f.isClosed():
    let docs = waitFor f.refresh()
    for doc in docs:
      yield doc

proc next*(f: Cursor[Mongo]): seq[Bson] =
  ## Perform MongoDB query for next batch of documents
  return f.refresh()

proc next*(f: Cursor[AsyncMongo]): Future[seq[Bson]] {.async.} =
  ## Perform MongoDB query for next batch of documents
  let docs = await f.refresh()
  result = docs

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
  if maxDocs > 0: request["max"] = maxDocs.toBson()

  let response = db["$cmd"].makeQuery(request).one()
  return response.toStatusReply

proc createCollection*(db: Database[AsyncMongo], name: string, capped: bool = false, autoIndexId: bool = true, maxSize: int = 0, maxDocs: int = 0): Future[StatusReply] {.async.} =
  ## Create collection inside database via async connection
  var request = %*{"create": name}

  if capped: request["capped"] = capped.toBson()
  if autoIndexId: request["autoIndexId"] = true.toBson()
  if maxSize > 0: request["size"] = maxSize.toBson()
  if maxDocs > 0: request["max"] = maxDocs.toBson()

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
  return f.collection.db["$cmd"].makeQuery(%*{"count": f.collection.name, "query": f.filter}).one().getReplyN

proc count*(f: Cursor[AsyncMongo]): Future[int] {.async.} =
  ## Return number of document in find query result via async connection
  let
    response = await f.collection.db["$cmd"].makeQuery(%*{
      "count": f.collection.name,
      "query": f.filter
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
      "query": f.filter,
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
      "query": f.filter,
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
      "writeConcern": if writeConcern == nil.Bson: c.writeConcern else: writeConcern
    }
    response = c.db["$cmd"].makeQuery(request).one()

  return response.toStatusReply(inserted_ids=inserted_ids)

proc insert*(c: Collection[Mongo], document: Bson, ordered: bool = true, writeConcern: Bson = nil): StatusReply {.discardable.} =
  ## Insert new document into MongoDB via sync connection
  return c.insert(@[document], ordered, if writeConcern == nil.Bson: c.writeConcern else: writeConcern)

proc insert*(c: Collection[AsyncMongo], documents: seq[Bson], ordered: bool = true, writeConcern: Bson = nil): Future[StatusReply] {.async.} =
  ## Insert new documents into MongoDB via async connection

  # 
  # insert any missing _id fields
  #
  var inserted_ids: seq[Bson] = @[]
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
      "writeConcern": if writeConcern == nil.Bson: c.writeConcern else: writeConcern
    }
    response = await c.db["$cmd"].makeQuery(request).one()

  return response.toStatusReply(inserted_ids=inserted_ids)

proc insert*(c: Collection[AsyncMongo], document: Bson, ordered: bool = true, writeConcern: Bson = nil): Future[StatusReply] {.async.} =
  ## Insert new document into MongoDB via async connection
  result = await c.insert(@[document], ordered, if writeConcern == nil.Bson: c.writeConcern else: writeConcern)

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
    "writeConcern": if writeConcern == nil.Bson: c.writeConcern else: writeConcern
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
    "writeConcern": if writeConcern == nil.Bson: c.writeConcern else: writeConcern
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
      "writeConcern": if writeConcern == nil.Bson: c.writeConcern else: writeConcern
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
      "writeConcern": if writeConcern == nil.Bson: c.writeConcern else: writeConcern
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
