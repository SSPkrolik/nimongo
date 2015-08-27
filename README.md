nimongo - Pure Nim MongoDB Driver
===================================

`nimongo` has a main intention to provide developer-friendly way to interact
with MongoDB using Nim programming language without any other dependencies.

Installation
------------
You can use `nimble` package manager to install `nimongo`.

Usage
-----

```nim
import oids
import nimongo.mongo

## Create new Mongo clinet
var m = new Mongo()

## Connect to Mongo server
let connectResult = m.connect()

## Specify needed collection
let collection = m["db"]["collectionName"]

## Create new bson document
let doc = B(
    "_id", genOid())(
    "name", "John")(
    "skills", B(
        "Nim", "good")(
        "JavaScript", "so-so"
        )
    )

## Insert document into DB
collection.insert(doc)
```
