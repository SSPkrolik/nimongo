import "strutils"
import "unsigned"

type
  Mongo* = ref object
    host: string
    port: uint16

proc newMongo(): Mongo =
  result.new
  result.host = "127.0.0.1"
  result.port = 27017

proc `$`(mongo: Mongo): string =
  return "mongodb://$#:$#" % [mongo.host, $mongo.port]

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
