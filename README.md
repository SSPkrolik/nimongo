nimongo - Pure Nim MongoDB Driver
===================================

`nimongo` has a main intention to provide developer-friendly way to interact
with MongoDB using Nim programming language without any other dependencies.

Installation
------------
You can use `nimble` package manager to install `nimongo`. The most recent
version of the library can be installed like this:

```bash
$ nimble install https://github.com/SSPkrolik/nimongo.git
```

Later, when the driver becomes usable, `nimongo` will enter `nimble` package
list.

> WARNING! Current `master` version of `nimongo` works only with the
> latest version of Nim from git repo: `devel` branch.

Usage
-----

```nim
import oids

import nimongo.bson  ## MongoDB BSON serialization/deserialization
import nimongo.mongo ## MongoDB clinet

## Create new Mongo client
var m = newMongo().slaveOk(true).allowPartial(false)

## Connect to Mongo server
let connectResult = m.connect()

## Specify collection
let collection = m["db"]["collectionName"]

## Create new bson document
let doc = B("name", "John")

## Insert document into DB
collection.insert(doc)

## Update [single] document
collection.update(B("name", "John"), B("$set", B("surname", "Smith")))

## Delete document
collection.remove(B("name", "John"))

## Fetch one document from DB returning only one field: "name".
let fetched = collection.find(B("name", "John"), @["name"]).one()

## Fetch all matching documents from DB receiving seq[Bson]
let documents = collection.find(B("name", "John")).all()

## Fetch all matching documents as a iterator
for document in collection.find(B("name", "John")).items():
    echo document
```
