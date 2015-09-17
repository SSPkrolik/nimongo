nimongo - Pure Nim MongoDB Driver
===================================

`nimongo` has a main intention to provide developer-friendly way to interact
with MongoDB using Nim programming language without any other dependencies.

You can find a table of supported features at the bottom of the document.

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

Usage of synchronous client
---------------------------
`nimongo.mongo.Mongo` synchronous client perform interaction with MongoDB
over network using sockets and __blocking__ I/O which stops the thread it is
used on from executing while MongoDB operation is not finished: either data
is sent over network (_insert_, _update_, _remove_), or query (_find_) is done,
and answer (or portion of it) is waited for.

Mongo synchronous client is __thread-safe__. It uses simple `Lock` when
executing commands and queries.

```nim
import oids

import nimongo.bson  ## MongoDB BSON serialization/deserialization
import nimongo.mongo ## MongoDB client

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

Usage of async client
---------------------
`nimongo.mongo.Mongo` can also work in an asynchronous mode based on
`ayncdispatch` standard async mechanisms, and `asyncnet.AsyncSocket` sockets. It
performs non-blocking I/O via `{.async.}` procedures.

Mongo async client is __thread-safe__. It uses simple `Lock` when
executing commands and queries.

```nim
import asyncdispatch  ## Nim async-supportive functions here
import oids

import nimongo.bson   ## MongoDB BSON serialization/deserialization
import nimongo.mongo  ## MongoDB client

## Create new Mongo client
var m: Mongo = newAsyncMongo().slaveOk(false)  ## Still Mongo type

## Connect to Mongo server with asynchronous socket
let connected = waitFor(m.asyncConnect())

## Testing connection establishing result
echo "Async connection established: ", connected

## Inserting single document into MongoDB
waitFor(m.asyncInsert(B("hello-async", "victory")))

## Inserting multiple documents into MongoDB
let
  doc1 = B("doc1", 15)
  doc2 = B("doc2", "string")

waitFor(m.asyncInsert(@[doc1, doc2]))

## Removing single document from MongoDB
waitFor(m.asyncRemove(B("doc1", 15)))
```

Currently Supported Features
============================
Here's a list of supported features with appropriate status icons:

 * :white_check_mark: - implemented feature
 * :red_circle: - __not__ implemented feature
 * :warning: - __partly__ supported or __unstable__

BSON
----
This table represents BSON data types and their implementation status
within `nimongo.bson` Nim module.

| Block      | Data Type       | Status             |
|-----------:|:----------------|:------------------:|
| Data Types |                 |                    |
|            | Double          | :white_check_mark: |
|            | String          | :white_check_mark: |
|            | SubSocument     | :white_check_mark: |
|            | String          | :white_check_mark: |
|            | Array           | :white_check_mark: |
|            | Mongo ObjectId  | :white_check_mark: |
|            | Undefined       | :white_check_mark: |
|            | Boolean         | :white_check_mark: |
|            | UTC datetime    | :white_check_mark: |
|            | Null            | :white_check_mark: |
|            | RegExp          | :white_check_mark: |
|            | DBRef           | :white_check_mark: |
|            | JavaScript code | :white_check_mark: |
|            | JavaScript code w/ scope|:red_circle:|
|            | Int32           | :white_check_mark: |
|            | Timestamp (inner)|:red_circle:       |
|            | Int64           | :white_check_mark: |
|            | Minimum Key     | :white_check_mark: |
|            | Maximum Key     | :white_check_mark: |
| Binary Subtypes |            |                    |
|            | Generic         | :white_check_mark: |
|            | Binary (Old)    | :red_circle:       |
|            | UUID (Old)      | :red_circle:       |
|            | UUID            | :red_circle:       |
|            | MD5             | :white_check_mark: |
|            | User-defined    | :red_circle:       |

MongoDB Features
----------------
This table represents MongoDB features and their implementation status within
`nimongo.mongo` Nim module.

| Block      | Feature         | Status (sync)      | Status (async)     |
|-----------:|:----------------|:------------------:|:------------------:|
| Operations |                 | __4__:__0__/__7__  | __3__:__0__/__7__  |
|            | Insert          | :white_check_mark: | :white_check_mark: |
|            | Multiple Insert | :white_check_mark: | :white_check_mark: |
|            | Remove          | :white_check_mark: | :white_check_mark: |
|            | Remove multiple | :red_circle:       | :red_circle:       |
|            | Update          | :white_check_mark: | :red_circle:       |
|            | Update multiple | :red_circle:       | :red_circle:       |
|            | Upsert          | :red_circle:       | :red_circle:       |
| Querying   |                 | __1__:__5__/__6__  | __0__:__0__/__6__  |
|            | Find one        | :white_check_mark: | :red_circle:       |
|            | Find            | :warning:          | :red_circle:       |
|            | Skip            | :warning:          | :red_circle:       |
|            | Limit           | :warning:          | :red_circle:       |
|            | Tailable        | :warning:          | :red_circle:       |
|            | Partial         | :warning:          | :red_circle:       |
| Commands   |                 | __1__:__0__/__1__  | __0__:__0__/__1__  |
|            | isMaster        | :white_check_mark: | :red_circle:       |
| Replica    |                 | __0__:__0__/__1__  | __0__:__0__/__1__  |
|            |                 | :red_circle:       | :red_circle:       |
| GridFS     |                 | __0__:__0__/__1__  | __0__:__0__/__1__  |
|            |                 | :red_circle:       | :red_circle:       |
| Indices    |                 | __0__:__0__/__4__  | __0__:__0__/__4__  |
|            | Create Index    | :red_circle:       | :red_circle:       |
|            | Drop Index      | :red_circle:       | :red_circle:       |
|            | Drop Indices    | :red_circle:       | :red_circle:       |
|            | Ensure Index    | :red_circle:       | :red_circle:       |
|Aggreagation|                 | __0__:__0__/__1__  | __0__:__0__/__1__  |
|            | Aggregate       | :red_circle:       | :red_circle:       |
| Map/Reduce |                 | __0__:__0__/__1__  | __0__:__0__/__1__  |
|            | MapReduce       | :red_circle:       | :red_circle:       |

__P.S.__ Contribution is welcomed :)
