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

## Delete multiple documents
collection.remove(B("name", "John"), RemoveMultiple)

## Delete single document
collection.remove(B("name", "John"), RemoveSingle)

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
var m: AsyncMongo = newAsyncMongo().slaveOk(false)  ## Still Mongo type

## Connect to Mongo server with asynchronous socket
let connected = waitFor(m.connect())

## Testing connection establishing result
echo "Async connection established: ", connected

## Inserting single document into MongoDB
waitFor(m.insert(B("hello-async", "victory")))

## Inserting multiple documents into MongoDB
let
  doc1 = B("doc1", 15)
  doc2 = B("doc2", "string")

waitFor(m.insert(@[doc1, doc2]))

## Removing single document from MongoDB
waitFor(m.remove(B("doc1", 15), RemoveSingle))

## Removing multiple documents from MongoDB
waitFor(m.remove(B("doc1", 15), RemoveMultiple))
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
|Connection  |                 | __1__ / __7__      | __1__ / __7__      |
|            | Single server   | :white_check_mark: | :white_check_mark: |
|            | Replica set     | :red_circle:       | :red_circle:       |
|            | SSL             | :red_circle:       | :red_circle:       |
|            | Socket Timeout  | :red_circle:       | :red_circle:       |
|            | Connect Timeout | :red_circle:       | :red_circle:       |
|            | Write Concern   | :red_circle:       | :red_circle:       |
|            | Read Preference | :red_circle:       | :red_circle:       |
|Operations  |                 | __7__ / __7__      | __7__ / __7__      |
|            | Insert          | :white_check_mark: | :white_check_mark: |
|            | Multiple Insert | :white_check_mark: | :white_check_mark: |
|            | Remove single   | :white_check_mark: | :white_check_mark: |
|            | Remove multiple | :white_check_mark: | :white_check_mark: |
|            | Update single   | :white_check_mark: | :white_check_mark: |
|            | Update multiple | :white_check_mark: | :white_check_mark: |
|            | Upsert          | :white_check_mark: | :white_check_mark: |
|Querying    |                 | __6__ / __10__     | __5__ / __9__      |
|            | Find one        | :white_check_mark: | :white_check_mark: |
|            | Find all        | :white_check_mark: | :white_check_mark: |
|            | Find iterator   | :white_check_mark: | __N/A__            |
|            | Skip            | :white_check_mark: | :white_check_mark: |
|            | Limit           | :white_check_mark: | :white_check_mark: |
|            | Count           | :white_check_mark: | :white_check_mark: |
|            | Tailable        | :red_circle:       | :red_circle:       |
|            | Partial         | :red_circle:       | :red_circle:       |
|            | FindAndModify   | :red_circle:       | :red_circle:       |
|            | parallelCollectionScan | :red_circle:| :red_circle:       |
|Errors      |                 | __0__ / __3__      | __0__ / __3__      |
|            | getLastError    | :red_circle:       | :red_circle:       |
|            | getPrevError    | :red_circle:       | :red_circle:       |
|            | resetError      | :red_circle:       | :red_circle:       |
|Authentication |              | __0__ / __7__      | __0__ / __7__      |
|            | authenticate    | :red_circle:       | :red_circle:       |
|            | SCRAM-SHA-1     | :red_circle:       | :red_circle:       |
|            | MONGODB-CR      | :red_circle:       | :red_circle:       |
|            | MONGODB-X509    | :red_circle:       | :red_circle:       |
|            | GSSAPI (Kerberos)| :red_circle:      | :red_circle:       |
|            | PLAIN (LDAP SASL)| :red_circle:      | :red_circle:       |
|            | logout          | :red_circle:       | :red_circle:       |
|User Management |             | __0__ / __7__      | __0__ / __7__      |
|            | Create User     | :red_circle:       | :red_circle:       |
|            | Update User     | :red_circle:       | :red_circle:       |
|            | Drop User       | :red_circle:       | :red_circle:       |
|            | Drop all users  | :red_circle:       | :red_circle:       |
|            | Grant roles     | :red_circle:       | :red_circle:       |
|            | Revoke roles    | :red_circle:       | :red_circle:       |
|            | Users info      | :red_circle:       | :red_circle:       |
|Role Management |             | __0__ / __0__      | __0__ / __0__      |
|Replication |                 | __1__ / __1__      | __1__ / __1__      |
|            | Is Master       | :white_check_mark: | :white_check_mark: |
|Sharding    |                 | __0__ / __0__      | __0__ / __0__      |
|Admin Commands|               | __3__ / __8__      | __3__ / __8__      |
|            | Rename collection|:red_circle:       | :red_circle:       |
|            | Copy DB         | :red_circle:       | :red_circle:       |
|            | List databases  | :white_check_mark: | :white_check_mark: |
|            | Drop database   | :white_check_mark: | :white_check_mark: |
|            | List collections| :red_circle:       | :red_circle:       |
|            | Rename collection|:red_circle:       | :red_circle:       |
|            | Drop collection | :white_check_mark: | :white_check_mark: |
|            | Create collection|:red_circle:       | :red_circle:       |
|Diagnostic  |                 | __0__ / __0__      | __0__ / __0__      |
|GridFS      |                 | __0__ / __0__      | __0__ / __0__      |
|Indices     |                 | __0__ / __4__      | __0__ / __4__      |
|            | Create Index    | :red_circle:       | :red_circle:       |
|            | Drop Index      | :red_circle:       | :red_circle:       |
|            | Drop Indices    | :red_circle:       | :red_circle:       |
|            | Ensure Index    | :red_circle:       | :red_circle:       |
|Aggregation |                 | __1__ / __5__      | __1__ / __5__      |
|            | aggregate       | :red_circle:       | :red_circle:       |
|            | count           | :white_check_mark: | :white_check_mark: |
|            | distinct        | :red_circle:       | :red_circle:       |
|            | group           | :red_circle:       | :red_circle:       |
|            | mapReduce       | :red_circle:       | :red_circle:       |
|Geospatial  |                 | __0__ /__3__       | __0__ / __3__      |
|            | geoNear         | :red_circle:       | :red_circle:       |
|            | geoSearch       | :red_circle:       | :red_circle:       |
|            | geoWalk         | :red_circle:       | :red_circle:       |
|Auditing    |                 | __0__ / __1__      | __0__ / __1__      |
|            |logApplicationMessage|:red_circle:    | :red_circle:       |

__P.S.__ Contribution is welcomed :)
