# Required for using _Lock on linux
when hostOs == "linux":
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
    #RESERVED       = 2003'i32 ## Reserved by MongoDB developers
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

proc connect*(m: Mongo): bool =
    ## Connect socket to mongo server
    try:
        m.sock.connect(m.host, Port(m.port), -1)
    except OSError:
        return false
    return true

proc insert*(c: Collection, document: Bson): bool {.discardable.} =
    ## Insert new document into MongoDB
    let
        sdoc = document.bytes()
        msgHeader = buildMessageHeader(int32(21 + len($c) + sdoc.len()), nextRequestId(), 0, OP_INSERT)

    return c.client.sock.trySend(msgHeader & buildMessageInsert(0, $c) & sdoc)

proc insert*(c: Collection, documents: seq[Bson], continueOnError: bool = false): bool {.discardable.} =
    ## Insert several new documents into MongoDB using one request
    assert len(documents) > 0

    var total = 0
    let sdocs: seq[string] = mapIt(documents, string, bytes(it))
    for sdoc in sdocs: inc(total, sdoc.len())

    let msgHeader = buildMessageHeader(int32(21 + len($c) + total), nextRequestId(), 0, OP_INSERT)

    return c.client.sock.trySend(msgHeader & buildMessageInsert(if continueOnError: 1 else: 0, $c) & foldl(sdocs, a & b))

proc remove*(c: Collection, selector: Bson): bool {.discardable.} =
    ## Delete documents from MongoDB
    let
        sdoc = selector.bytes()
        msgHeader = buildMessageHeader(int32(25 + len($c) + sdoc.len()), nextRequestId(), 0, OP_DELETE)

    return c.client.sock.trySend(msgHeader & buildMessageDelete(0, $c) & sdoc)

proc update*(c: Collection, selector: Bson, update: Bson): bool {.discardable.} =
    ## Update MongoDB document[s]
    let
        ssel = selector.bytes()
        supd = update.bytes()
        msgHeader = buildMessageHeader(int32(25 + len($c) + ssel.len() + supd.len()), nextRequestId(), 0, OP_UPDATE)

    return c.client.sock.trySend(msgHeader & buildMessageUpdate(0, $c) & ssel & supd)
