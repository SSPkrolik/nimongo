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
    check doc["stringkey"] is Bson
    check(doc["stringkey"].string == "stringvalue")
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
    doc["int32"] = toBson(2'i32)
    check(doc["int32"] == 2'i32)
    doc["array"][0] = toBson(10'i32)
    check(doc["array"][0] == 10'i32)
    doc["newfield"] = "newvalue".toBson
    check(doc["newfield"] == "newvalue")


  test "Check if document has specific field with `in` operator":
    let doc = %*{
      "field1": "string",
      "field2": 1'i32
    }
    check("field1" in doc)
    check(not ("field3" in doc))

  test "Document inside array":
    let doc = %*{
      "field": "value",
      "ar": [
        {
          "field1": 5'i32,
          "field2": "gello"
        },
        {
          "field": "hello"
        }
      ]
    }
    check(doc["ar"][0]["field1"].toInt() == 5)

  test "Document's merge":
    let a = %*{
      "field1": "value1",
      "field2": [
        {"ar0": "1"},
        {"ar1": "2"},
        {"ar2": "3"}
      ]
    }
    let b = %*{
      "field3": "value2",
      "field0": 5'i32
    }

    let abm = merge(a, b)
    check(abm["field0"] == 5'i32)
    check(abm["field2"][0]["ar0"] == "1")

  test "Document update":
    let a = %*{
      "field1": "value1",
      "field2": [
        {"ar0": "1"},
        {"ar1": "2"},
        {"ar2": "3"}
      ]
    }

    let b = %*{
      "field3": "value2",
      "field0": 5'i32
    }

    b.update(a)
    check(b["field0"] == 5'i32)
    check(b["field2"][0]["ar0"] == "1")

  test "array length":
    let arr = newBsonArray()
    arr.add(%*{
      "field3": "value2",
      "field0": 5'i32
    })

    check(arr.len == 1)

