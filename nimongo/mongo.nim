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
    # OP_MSG        = 1000'i32 ## Deprecated.
    OP_UPDATE       = 2001'i32 ##
    OP_INSERT       = 2002'i32 ## Insert new document into MongoDB
    # RESERVED      = 2003'i32 ## Reserved by MongoDB developers
    OP_QUERY        = 2004'i32 ##
    OP_GET_MORE     = 2005'i32 ##
    OP_DELETE       = 2006'i32 ## Remove documents from MongoDB
    OP_KILL_CURSORS = 2007'i32 ##

const
    TailableCursor   = 1'i32 shl 1 ## Leave cursor alive on MongoDB side
    SlaveOk          = 1'i32 shl 2 ## Allow to query replica set slaves
    NoCursorTimeout  = 1'i32 shl 4 ##
    AwaitData        = 1'i32 shl 5 ##
    Exhaust          = 1'i32 shl 6 ##
    Partial          = 1'i32 shl 7 ## Get info only from running shards

converter toInt32(ok: OperationKind): int32 =  ## Convert OperationKind ot int32
    return ok.int32

type
    Mongo* = ref object ## Mongo represents connection to MongoDB server
        requestLock: Lock
        requestId:   int32
        host:        string
        port:        uint16
        sock:        Socket
        queryFlags:  int32

    Database* = ref object ## MongoDB database object
        name:   string
        client: Mongo

    Collection* = ref object ## MongoDB collection object
        name:   string
        db:     Database
        client: Mongo

proc nextRequestId(m: Mongo): int32 =
    ## Return next request id for current MongoDB client
    {.locks: [m.requestLock].}:
        m.requestId = (m.requestId + 1) mod (int32.high - 1'i32)
        return m.requestId

proc newMongo*(host: string = "127.0.0.1", port: uint16 = 27017): Mongo =
    ## Mongo constructor
    result.new
    result.host = host
    result.port = port
    result.requestID = 0
    result.sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, true)

proc slaveOk*(m: Mongo, enable: bool = true): Mongo {.discardable.} =
    ## Enable/disable querying from slaves in replica sets
    m.queryFlags = if enable: m.queryFlags or SlaveOk else: m.queryFlags and (not SlaveOk)
    return m

proc allowPartial*(m: Mongo, enable: bool = true): Mongo {.discardable} =
    ## Enable/disable allowance for partial data retrieval from mongos when
    ## one or more shards are down.
    m.queryFlags = if enable: m.queryFlags or Partial else: m.queryFlags and (not Partial)

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
        msgHeader = buildMessageHeader(int32(21 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_INSERT)

    return c.client.sock.trySend(msgHeader & buildMessageInsert(0, $c) & sdoc)

proc insert*(c: Collection, documents: seq[Bson], continueOnError: bool = false): bool {.discardable.} =
    ## Insert several new documents into MongoDB using one request
    assert len(documents) > 0

    var total = 0
    let sdocs: seq[string] = mapIt(documents, string, bytes(it))
    for sdoc in sdocs: inc(total, sdoc.len())

    let msgHeader = buildMessageHeader(int32(21 + len($c) + total), c.client.nextRequestId(), 0, OP_INSERT)

    return c.client.sock.trySend(msgHeader & buildMessageInsert(if continueOnError: 1 else: 0, $c) & foldl(sdocs, a & b))

proc remove*(c: Collection, selector: Bson): bool {.discardable.} =
    ## Delete documents from MongoDB
    let
        sdoc = selector.bytes()
        msgHeader = buildMessageHeader(int32(25 + len($c) + sdoc.len()), c.client.nextRequestId(), 0, OP_DELETE)

    return c.client.sock.trySend(msgHeader & buildMessageDelete(0, $c) & sdoc)

proc update*(c: Collection, selector: Bson, update: Bson): bool {.discardable.} =
    ## Update MongoDB document[s]
    let
        ssel = selector.bytes()
        supd = update.bytes()
        msgHeader = buildMessageHeader(int32(25 + len($c) + ssel.len() + supd.len()), c.client.nextRequestId(), 0, OP_UPDATE)

    return c.client.sock.trySend(msgHeader & buildMessageUpdate(0, $c) & ssel & supd)

proc find*(c: Collection, selector: Bson, fields: seq[string] = @[]): seq[Bson] =
    ## Query documents from MongoDB
