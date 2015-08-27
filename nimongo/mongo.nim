{.passL: "-pthread".}

import locks
import oids
import sequtils
import sockets
import strutils
import tables
import unsigned
import json

import bson

type OperationKind = enum      ## Type of operation performed by MongoDB
    OP_REPLY        =    1'i32 ##
    OP_MSG          = 1000'i32 ##
    OP_UPDATE       = 2001'i32 ##
    OP_INSERT       = 2002'i32 ## Insert new document into MongoDB
    #RESERVED       = 2003'i32 ##
    OP_QUERY        = 2004'i32 ##
    OP_GET_MORE     = 2005'i32 ##
    OP_DELETE       = 2006'i32 ##
    OP_KILL_CURSORS = 2007'i32 ##

converter toInt32(ok: OperationKind): int32 =
    return ok.int32

var
    MongoRequestIDLock: Lock
    MongoRequestID {.guard: MongoRequestIDLock.}: int32 = 0

proc nextRequestId(): int32 =
    ## Return next request id
    {.locks: [MongoRequestIDLock].}:
        MongoRequestID = (MongoRequestID + 1) mod (int32.high - 1'i32)
        return MongoRequestID

type
    Mongo* = ref object ## Mongo represents connection to MongoDB server
        requestID: int32
        host: string
        port: uint16
        sock: Socket

    Database* = ref object ## MongoDB database object
        name: string
        client: Mongo

    Collection* = ref object ## MongoDB collection object
        name: string
        db: Database
        client: Mongo

    MongoMessageHeader = object ## MongoDB network (wire) protocol message header
        messageLength: int32
        requestID: int32
        responseTo: int32
        opCode: int32

    MongoMessageInsert = object ## Structure of OP_INSERT operation
        flags: int32
        fullCollectionName: string

    MongoMessageDelete = object ## Structure of OP_DELETE operation
        ZERO: int32
        fullCollectionName: string
        flags: int32

    MongoMessageUpdate = object ## Structure of OP_UPDATE operation
        ZERO: int32
        fullCollectionName: string
        flags: int32

proc initMongoMessageHeader(responseTo: int32, opCode: int32): MongoMessageHeader =
    return MongoMessageHeader(
        messageLength: 16,
        requestID: nextRequestId(),
        responseTo: responseTo,
        opCode: opCode
    )

proc initMongoMessageInsert(coll: string): MongoMessageInsert =
    return MongoMessageInsert(
        flags: 0,
        fullCollectionName: coll
    )

proc initMongoMessageDelete(coll: string): MongoMessageDelete =
    return MongoMessageDelete(
        ZERO: 0,
        fullCollectionName: coll,
        flags: 0
    )

proc initMongoMessageUpdate(coll: string): MongoMessageUpdate =
    return MongoMessageUpdate(
        ZERO: 0,
        fullCollectionName: coll,
        flags: 0
    )

proc `$`(mmh: MongoMessageHeader): string =
    return int32ToBytes(mmh.messageLength) & int32ToBytes(mmh.requestId) & int32ToBytes(mmh.responseTo) & int32ToBytes(mmh.opCode)

proc `$`(mmi: MongoMessageInsert): string =
    return int32ToBytes(mmi.flags) & mmi.fullCollectionName & char(0)

proc `$`(mmd: MongoMessageDelete): string =
    return int32ToBytes(mmd.ZERO) & mmd.fullCollectionName & char(0) & int32ToBytes(mmd.flags)

proc `$`(mmu: MongoMessageUpdate): string =
    return int32ToBytes(mmu.ZERO) & mmu.fullCollectionName & char(0) & int32ToBytes(mmu.flags)

proc `$`*(c: Collection): string =
    ## String representation of collection name
    return c.db.name & "." & c.name

proc `$`*(db: Database): string = db.name  ## Database name

proc `[]`*(m: Mongo, dbName: string): Database =
    ## Retrieves database from Mongo
    result.new
    result.name = dbName
    result.client = m

proc `[]`*(db: Database, collectionName: string): Collection =
    ## Retrieves collection from Mongo Database
    result.new
    result.name = collectionName
    result.client = db.client
    result.db = db

proc newMongo*(host: string = "127.0.0.1", port: uint16 = 27017): Mongo =
    ## Mongo constructor
    result.new
    result.host = host
    result.port = port
    result.requestID = nextRequestID()
    result.sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, true)

proc `$`*(m: Mongo): string =
    ## Return full DSN for the Mongo connection
    return "mongodb://$#:$#" % [m.host, $m.port]

proc connect(m: Mongo): bool =
    ## Connect socket to mongo server
    try:
        m.sock.connect(m.host, Port(m.port), -1)
    except OSError:
        return false
    return true

proc insert*(c: Collection, document: Bson) =
    ## Insert new document into MongoDB
    var
        msgHeader = initMongoMessageHeader(0, OP_INSERT)
        msgInsert = initMongoMessageInsert($c)
        sdoc = document.bytes()
    msgHeader.messageLength = int32(21 + len(msgInsert.fullCollectionName) + sdoc.len())

    let data: string = $msgHeader & $msgInsert & sdoc

    for c in data:
        stdout.write(ord(c), " ")
    echo ""
    if c.client.sock.trySend(data):
        echo "Successfully sent!"

proc insert*(c: Collection, documents: seq[Bson]) =
    ## Insert several new documents into MongoDB using one request
    assert len(documents) > 0

    var
        msgHeader = initMongoMessageHeader(0, OP_INSERT)
        msgInsert = initMongoMessageInsert($c)
        total = 0
    let
        sdocs: seq[string] = mapIt(documents, string, bytes(it))

    for sdoc in sdocs: inc(total, sdoc.len())

    msgHeader.messageLength = int32(21 + len(msgInsert.fullCollectionName) + total)

    let data: string = $msgHeader & $msgInsert

    if c.client.sock.trySend(data & foldl(sdocs, a & b)):
        echo "Successfully sent!"

proc remove*(c: Collection, selector: Bson) =
    ## Delete documents from MongoDB
    var
        msgHeader = initMongoMessageHeader(0, OP_DELETE)
        msgDelete = initMongoMessageDelete($c)
    let
        sdoc = selector.bytes()
    msgHeader.messageLength = int32(25 + len(msgDelete.fullCollectionName) + sdoc.len())

    if c.client.sock.trySend($msgHeader & $msgDelete & sdoc):
        echo "OP_DELETE successfully sent!"

proc update*(c: Collection, selector: Bson, update: Bson) =
    ## Update MongoDB documents
    var
        msgHeader = initMongoMessageHeader(0, OP_UPDATE)
        msgUpdate = initMongoMessageUpdate($c)

    let
        ssel = selector.bytes()
        supd = update.bytes()

    msgHeader.messageLength = int32(25 + len(msgUpdate.fullCollectionName) + ssel.len() + supd.len())

    if c.client.sock.trySend($msgHeader & $msgUpdate & ssel & supd):
        echo "OP_UPDATE successfully sent!"

when isMainModule:
  let unittest = proc(): bool =
    ## Test object
    var m: Mongo = newMongo()

    ## Test () constructor
    m = newMongo()
    assert(m.host == "127.0.0.1")
    assert(m.port == uint16(27017))

    ## Test `$` operator
    assert($m == "mongodb://127.0.0.1:27017")

    let connectResult: bool = m.connect()
    if not connectResult:
        return false

    let c = m["falcon"]["profiles"]
    echo c

    #let langs: seq[Bson] = mapIt(@["Python", "Ruby", "C"], toBson(it))
    #let doc = initBsonDocument()(
    #    "balance", 500)(
    #    "_id", genOid())(
    #    "languages", @["Python", "Ruby", "C", "CPP"])(
    #    "skills", initBsonDocument()(
    #        "C++", 10)(
    #        "Python", 20'i32)
    #    )
    #let doc = initBsonDocument()("languages", @["Python", "Ruby", "C"].mapIt(Bson, toBson(it)))

    let docs = @[initBsonDocument()("balance", 100.23), initBsonDocument()("balance", 15'i32)]
    echo docs

    let sel = initBsonDocument()("balance", initBsonDocument()("$lt", 20))
    echo sel

    c.update(
        B("balance", 15),
        B("$set", B("balance", "UPDATED"))
    )

    c.insert(docs)
    #c.remove(sel)

    return true

  if unittest():
    echo "TEST SUCCESS!"
