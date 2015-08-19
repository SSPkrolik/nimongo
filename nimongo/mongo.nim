import sockets
import strutils
import tables
import unsigned

import bson

type
    Mongo* = ref object ## Mongo represents connection to MongoDB server
        host: string
        port: uint16
        sock: Socket

    Database* = ref object ## MongoDB database object
        name: string

    Collection* = ref object ## MongoDB collection object
        name: string

proc `$`*(db: Database): string = db.name  ## Database name

proc `[]`*(m: Mongo, dbName: string): Database =
    ## Retrieves database from Mongo
    result.new
    result.name = dbName

proc `[]`*(db: Database, collectionName: string): Collection =
    ## Retrieves collection from Mongo Database
    result.new
    result.name = collectionName

proc insert*(c: Collection, o: Table) =
    discard

proc newMongo*(host: string = "127.0.0.1", port: uint16 = 27017): Mongo =
    ## Mongo constructor
    result.new
    result.host = host
    result.port = port
    result.sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP, true)

proc `$`*(m: Mongo): string =
    ## Return full DSN for the Mongo connection
    return "mongodb://$#:$#" % [m.host, $m.port]

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

    return true

  if unittest():
    echo "TEST SUCCESS!"
