import os
import strutils
import uri
import ../bson except `()`
import writeconcern
import proto

when compileOption("threads"):
    import locks

const
    DefaultMongoHost* = "127.0.0.1"
    DefaultMongoPort* = 27017'u16  ## Default MongoDB IP Port

    TailableCursor*  = 1'i32 shl 1 ## Leave cursor alive on MongoDB side
    SlaveOk*         = 1'i32 shl 2 ## Allow to query replica set slaves
    NoCursorTimeout* = 1'i32 shl 4 ##
    AwaitData*       = 1'i32 shl 5 ##
    Exhaust*         = 1'i32 shl 6 ##
    Partial*         = 1'i32 shl 7 ## Get info only from running shards

    RFCursorNotFound*    = 1'i32       ## CursorNotFound. Is set when getMore is called but the cursor id is not valid at the server. Returned with zero results.
    RFQueryFailure*      = 1'i32 shl 1 ## QueryFailure. Is set when query failed. Results consist of one document containing an “$err” field describing the failure.
    RFShardConfigStale*  = 1'i32 shl 2 ## ShardConfigStale. Drivers should ignore this. Only mongos will ever see this set, in which case, it needs to update config from the server.
    RFAwaitCapable*      = 1'i32 shl 3 ## AwaitCapable. Is set when the server supports the AwaitData Query option. If it doesn’t, a client should sleep a little between getMore’s of a Tailable cursor. Mongod version 1.6 supports AwaitData and thus always sets AwaitCapable.

type 
    ClientKind* = enum           ## Kind of client communication type
        ClientKindBase  = 0
        ClientKindSync  = 1
        ClientKindAsync = 2

    MongoBase* = ref object of RootObj ## Base for Mongo clients
        when compileOption("threads"):
            reqIdLock:  Lock
            requestId {.guard: reqIdLock.}: int32
        else:
            requestId:    int32
        host:         string
        port:         uint16
        queryFlags:   int32
        username:     string
        password:     string
        db:           string
        needAuth:     bool
        authenticated: bool
        replicas:     seq[tuple[host: string, port: uint16]]
        writeConcern: WriteConcern

    Database*[T] = ref DatabaseObj[T]
    DatabaseObj*[T] = object ## MongoDB database object
        name:   string
        client*: T

    Collection*[T] = ref CollectionObj[T]
    CollectionObj*[T] = object ## MongoDB collection object
        name:   string
        db:     Database[T]
        client: T

    CollectionInfo* = ref CollectionInfoObj
    CollectionInfoObj* = object  ## Collection information (for manual creation)
        disableIdIndex*: bool
        forceIdIndex*: bool
        capped: bool
        maxBytes: int
        maxDocs: int

    Cursor*[T] = ref CursorObj[T]
    CursorObj*[T] = object     ## MongoDB cursor: manages queries object lazily
        collection: Collection[T]
        query:      Bson
        fields:     seq[string]
        queryFlags*: int32
        nskip:      int32
        nlimit:     int32
        nbatchSize: int32
        sorting:    Bson
        cursorId:   int64
        count:      int32
        closed:     bool

    GridFS*[T] = ref GridFSObj[T]
    GridFSObj*[T] = object
        ## GridFS is collection which namespaced to .files and .chunks
        name*: string    # bucket name
        files*: Collection[T]
        chunks*: Collection[T]
    
    LockedSocketBase* {.inheritable.} = ref LockedSocketBaseObj
    LockedSocketBaseObj* {.inheritable.} = object
        inuse:         bool
        authenticated: bool
        connected:     bool

template lockIfThreads(body: untyped): untyped =
    when compileOption("threads"):
        mb.reqIdLock.acquire()
        try:
            {.locks: [mb.reqIdLock].}:
                body
        finally:
            mb.reqIdLock.release()
    else:
        body

method init*(mb: MongoBase, host: string, port: uint16) {.base.} =
    mb.host = host
    mb.port = port
    lockIfThreads:
        mb.requestID = 0
    mb.queryFlags = 0
    mb.replicas = @[]
    mb.username = ""
    mb.password = ""
    mb.db = "admin"
    mb.needAuth = false
    mb.authenticated = false
    mb.writeConcern = writeConcernDefault()

method init*(b: MongoBase, u: Uri) {.base.} =
    let port = if u.port.len > 0: parseInt(u.port).uint16 else: DefaultMongoPort
    b.init(u.hostname, port)
    b.username = u.username
    b.password = u.password
    let db = u.path.extractFilename()
    if db != "":
        b.db = db
    b.needAuth = (b.username != "" and b.db != "")

proc host*(mb: MongoBase): string = mb.host
    ## Connected server host

proc port*(mb: MongoBase): uint16 = mb.port
    ## Connected server port

proc username*(mb: MongoBase): string = mb.username
    ## Username to authenticate at Mongo Server

proc password*(mb: MongoBase): string = mb.password
    ## Password to authenticate at Mongo Server

proc authDb*(mb: MongoBase): string = mb.db
    ## Database for authentication

proc needsAuth*(mb: MongoBase): bool = mb.needAuth
    ## Check if connection needs to be authenticated

proc queryFlags*(mb: MongoBase): int32 = mb.queryFlags
    ## Query flags perform query flow and connection settings

proc `queryFlags=`*(mb: MongoBase, flags: int32) = mb.queryFlags = flags
    ## Query flags perform query flow and connection settings

proc nextRequestId*(mb: MongoBase): int32 =
    ## Return next request id for current MongoDB client
    lockIfThreads:
        mb.requestId = (mb.requestId + 1) mod (int32.high - 1'i32)
        result = mb.requestId

proc writeConcern*(mb: MongoBase): WriteConcern = mb.writeConcern
    ## Getter for currently setup client's write concern

proc `writeConcern=`*(mb: MongoBase, concern: WriteConcern) =
  ## Set client-wide write concern for sync client
  assert "w" in concern
  mb.writeConcern = concern

proc authenticated*(mb: MongoBase): bool = mb.authenticated
    ## Query authenticated flag

proc `authenticated=`*(mb: MongoBase, authenticated: bool) = mb.authenticated = authenticated
    ## Enable/disable authenticated flag for database

method kind*(mb: MongoBase): ClientKind {.base.} = ClientKindBase
    ## Base Mongo client

proc tailableCursor*(m: MongoBase, enable: bool = true): MongoBase {.discardable.} =
    ## Enable/disable tailable behaviour for the cursor (cursor is not
    ## removed immediately after the query)
    result = m
    m.queryFlags = if enable: m.queryFlags or TailableCursor else: m.queryFlags and (not TailableCursor)

proc slaveOk*(m: MongoBase, enable: bool = true): MongoBase {.discardable.} =
    ## Enable/disable querying from slaves in replica sets
    result = m
    m.queryFlags = if enable: m.queryFlags or SlaveOk else: m.queryFlags and (not SlaveOk)

proc noCursorTimeout*(m: MongoBase, enable: bool = true): MongoBase {.discardable.} =
    ## Enable/disable cursor idle timeout
    result = m
    m.queryFlags = if enable: m.queryFlags or NoCursorTimeout else: m.queryFlags and (not NoCursorTimeout)

proc awaitData*(m: MongoBase, enable: bool = true): MongoBase {.discardable.} =
    ## Enable/disable data waiting behaviour (along with tailable cursor)
    result = m
    m.queryFlags = if enable: m.queryFlags or AwaitData else: m.queryFlags and (not AwaitData)

proc exhaust*(m: MongoBase, enable: bool = true): MongoBase {.discardable.} =
    ## Enable/disabel exhaust flag which forces database to giveaway
    ## all data for the query in form of "get more" packages.
    result = m
    m.queryFlags = if enable: m.queryFlags or Exhaust else: m.queryFlags and (not Exhaust)

proc allowPartial*(m: MongoBase, enable: bool = true): MongoBase {.discardable} =
    ## Enable/disable allowance for partial data retrieval from mongos when
    ## one or more shards are down.
    result = m
    m.queryFlags = if enable: m.queryFlags or Partial else: m.queryFlags and (not Partial)

proc `$`*(m: MongoBase): string =
    ## Return full DSN for the Mongo connection
    return "mongodb://$#:$#" % [m.host, $m.port]

proc `[]`*[T: MongoBase](client: T, dbName: string): Database[T] =
    ## Retrieves database from Mongo
    result.new()
    result.name = dbName
    result.client = client

# === Locked Sockets API === #

method init*(ls: LockedSocketBase) {.base.} =
    ls.inuse = false
    ls.authenticated = false
    ls.connected = false

proc inuse*(ls: LockedSocketBase): bool = ls.inuse
    ## Return inuse 

proc `inuse=`*(ls: LockedSocketBase, inuse: bool) =
    ## Enable/disable inuse flag for socket
    ls.inuse = inuse

proc authenticated*(ls: LockedSocketBase): bool = ls.authenticated
    ## Return authenticated

proc `authenticated=`*(ls: LockedSocketBase, authenticated: bool) =
    ## Enable/disable authenticated flag for socket
    ls.authenticated = authenticated

proc connected*(ls: LockedSocketBase): bool = ls.connected
    ## Return connected

proc `connected=`*(ls: LockedSocketBase, connected: bool) =
    ## Enable/disable connected flag for socket
    ls.connected = connected

# === Database API === #

proc `$`*(db: Database): string =
    ## Database name string representation
    return db.name

proc `[]`*[T: MongoBase](db: Database[T], collectionName: string): Collection[T] =
    ## Retrieves collection from Mongo Database
    result.new()
    result.name = collectionName
    result.client = db.client
    result.db = db

proc name*(db: Database): string = db.name
    ## Return name of database

proc `name=`*(db: Database, name: string) =
    ## Set new database name
    db.name = name

# === Collection API === #

proc `$`*(c: Collection): string =
    ## String representation of collection name
    return c.db.name & "." & c.name

proc newCursor[T](c: Collection[T]): Cursor[T] =
    ## Private constructor for the Find object. Find acts by taking
    ## client settings (flags) that can be overriden when actual
    ## query is performed.
    result.new()
    result.collection = c
    result.fields = @[]
    result.queryFlags = c.client.queryFlags
    result.nskip = 0
    result.nlimit = 0
    result.nbatchSize = 0
    result.cursorId = 0
    result.count = 0
    result.closed = false

proc makeQuery*[T: MongoBase](c: Collection[T], query: Bson, fields: seq[string] = @[], maxTime: int32 = 0): Cursor[T] =
    ## Create lazy query object to MongoDB that can be actually run
    ## by one of the Find object procedures: `one()` or `all()`.
    result = c.newCursor()
    result.query = query
    result.fields = fields
    if maxTime > 0:
        result.query["$maxTimeMS"] = maxTime.toBson()


proc db*[T: MongoBase](c: Collection[T]): Database[T] = c.db
    ## Return the database from collection

proc name*(c: Collection): string = c.name
    ## Return name of collection

proc `name=`*(c: Collection, name: string) =
    ## Set new collection name
    c.name = name

proc writeConcern*(c: Collection): WriteConcern = c.client.writeConcern
    ## Return write concern for collection

# === Find API === #

proc prepareQuery*(f: Cursor, requestId: int32, numberToReturn: int32, numberToSkip: int32): string =
    ## Prepare query and request queries for making OP_QUERY
    var bfields: Bson = newBsonDocument()
    if f.fields.len() > 0:
        for field in f.fields.items():
            bfields[field] = 1'i32.toBson()
    let squery = f.query.bytes()
    let sfields: string = if f.fields.len() > 0: bfields.bytes() else: ""
    let colName = $(f.collection)
    result = ""
    var msg = ""
    buildMessageQuery(f.queryFlags, colName, numberToSkip, numberToReturn, msg)
    msg &= squery
    msg &= sfields
    buildMessageHeader(msg.len().int32, requestId, 0, OP_QUERY, result)
    result &= msg

proc prepareMore*(f: Cursor, requestId: int32, numberToReturn: int32): string =
    ## Prepare query and request queries for making OP_GET_MORE
    let colName = $(f.collection)
    result = ""
    var msg = ""
    buildMessageMore(colName, f.cursorId, numberToReturn, msg)
    buildMessageHeader(msg.len().int32, requestId, 0, OP_GET_MORE, result)
    result &= msg

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

proc skip*(f: Cursor, numSkip: int32): Cursor {.discardable.} =
    ## Specify number of documents from return sequence to skip
    result = f
    result.nskip = numSkip

proc limit*(f: Cursor, numLimit: int32): Cursor {.discardable.} =
    ## Specify number of documents to return from database
    result = f
    result.nlimit = numLimit # Should be negative if hard limit, else soft limit used
    
proc batchSize*(f: Cursor, numBatchSize: int32): Cursor {.discardable.} =
    ## Specify number of documents in first reply. Conflicts with limit
    result = f
    result.nbatchSize = numBatchSize

proc calcReturnSize*(f: Cursor): int32 =
    if f.nlimit == 0:
        result = f.nbatchSize
    elif f.nlimit < 0:
        result = f.nlimit
    else:
        result = f.nlimit - f.count
        if result <= 0:
            f.closed = true
            # TODO Add kill cursor functionality here
        if f.nbatchSize > 0:
            result = min(result, f.nbatchSize).int32

proc updateCount*(f: Cursor, count: int32) =
    ## Increasing the count of returned documents
    f.count += count

proc isClosed*(f: Cursor): bool = f.closed
    ## Return status of cursor

proc close*(f: Cursor) =
    ## Close cursor
    f.closed = true

proc `$`*(f: Cursor): string = $f.query
    ## Return query of cursor as a string

proc connection*[T: MongoBase](f: Cursor[T]): T = f.collection.client
    ## Get connection of cursor

proc collection*[T: MongoBase](f: Cursor[T]): Collection[T] = f.collection
    ## Get collection from cursor

proc cursorId*(f: Cursor): int64 = f.cursorId
    ## Return cursor ID

proc `cursorId=`*(f: Cursor, cursorId: int64) =
    ## Set cursor ID
    f.cursorId = cursorId

proc nskip*(f: Cursor): int32 = f.nskip
    ## Return amount of documents to skip

proc filter*(f: Cursor): Bson = f.query["$query"]
    ## Return filter of query from cursor
