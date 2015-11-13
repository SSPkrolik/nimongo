## Tests for bson.nim module
import unittest

import bson

suite "BSON serializer/deserializer test suite":

  echo "\n BSON serializer/deserializer test suite\n"

  test "Creating empty document with constructor":
    let doc = newBsonDocument()
    check($doc == "{\n}")

  test "Creating empty document with `%*` operator":
    let doc = %*{}
    check($doc == "{\n}")

  test "Creating document with all available types":
    let doc = %*{
      "double": 5436.5436,
      "stringkey": "stringvalue",
      "document": {
        "double": 5436.5436,
        "key": "value"
      },
      "array": [1, 2, 3],
      "int32": 5436'i32,
      "int64": 5436,
    }
    check(doc["double"] == 5436.5436)
    check(doc["stringkey"] == "stringvalue")
    check(doc["document"]["double"] == 5436.5436)
    check(doc["document"]["key"] == "value")
    check(doc["array"][0] == 1'i64)
    check(doc["int32"] == 5436'i32)
    check(doc["int64"] == 5436'i64)

  test "Document modification usin `[]=` operator":
    let doc = %*{
      "int32": 1'i32,
      "array": [1, 2, 3]
    }
    check(doc["int32"] == 1'i32)
    doc["int32"] = 2'i32
    check(doc["int32"] == 2'i32)
    doc["array"][0] = 10'i32
    check(doc["array"][0] == 10'i32)

echo ""
