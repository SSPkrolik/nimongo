import bson
import strutils
import uri
import writeconcern

const
    DefaultMongoHost* = "127.0.0.1"
    DefaultMongoPort* = 27017'u16  ## Default MongoDB IP Port

    TailableCursor*  = 1'i32 shl 1 ## Leave cursor alive on MongoDB side
    SlaveOk*         = 1'i32 shl 2 ## Allow to query replica set slaves
    NoCursorTimeout* = 1'i32 shl 4 ##
    AwaitData*       = 1'i32 shl 5 ##
    Exhaust*         = 1'i32 shl 6 ##
    Partial*         = 1'i32 shl 7 ## Get info only from running shards

type 
    ClientKind* = enum           ## Kind of client communication type
        ClientKindBase  = 0
        ClientKindSync  = 1
        ClientKindAsync = 2

    MongoBase* = ref object of RootObj ## Base for Mongo clients
        requestId:    int32
        host:         string
        port:         uint16
        queryFlags:   int32
        username:     string
        password:     string
        replicas:     seq[tuple[host: string, port: uint16]]
        writeConcern: WriteConcern

method init*(b: MongoBase, host: string, port: uint16) =
    b.host = host
    b.port = port
    b.requestID = 0
    b.queryFlags = 0
    b.replicas = @[]
    b.username = ""
    b.password = ""
    b.writeConcern = writeConcernDefault()

method init*(b: MongoBase, u: Uri) =
    let port = if u.port.len > 0: parseInt(u.port).uint16 else: DefaultMongoPort
    b.init(u.hostname, port)
    b.username = u.username
    b.password = u.password

proc host*(mb: MongoBase): string = mb.host
    ## Connected server host

proc port*(mb: MongoBase): uint16 = mb.port
    ## Connected server port

proc username*(mb: MongoBase): string = mb.username
    ## Username to authenticate at Mongo Server

proc password*(mb: MongoBase): string = mb.password
    ## Password to authenticate at Mongo Server

proc queryFlags*(mb: MongoBase): int32 = mb.queryFlags
    ## Query flags perform query flow and connection settings

proc `queryFlags=`*(mb: MongoBase, flags: int32) = mb.queryFlags = flags
    ## Query flags perform query flow and connection settings

proc nextRequestId*(mb: MongoBase): int32 =
    ## Return next request id for current MongoDB client
    mb.requestId = (mb.requestId + 1) mod (int32.high - 1'i32)
    return mb.requestId

proc writeConcern*(mb: MongoBase): WriteConcern =
    ## Getter for currently setup client's write concern
    return mb.writeConcern

proc `writeConcern=`*(mb: MongoBase, concern: WriteConcern) =
  ## Set client-wide write concern for sync client
  assert "w" in concern
  mb.writeConcern = concern

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
