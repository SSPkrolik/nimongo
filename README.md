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
$ nimble install nimongo
```

or directly from Git repo:

```bash
$ nimble install https://github.com/SSPkrolik/nimongo.git
```

> WARNING! Current `master` version of `nimongo` works only with the
> latest version of Nim from git repo: `devel` branch.
> Also nimongo is intended to support MongoDB versions >= 3.0.

Current status (briefly)
------------------------

Currently `nimongo.mongo` implements connection to single MongoDB server, and
support for most widely used queries (whole CRUD with some exceptions),

`nimongo.bson` gives full support of current BSON specification. As for
performance, it is comparable with `pymongo` Python driver on rough timeit-style
tests.

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
let doc = %*{
  "name": "John"
}



## Insert document into DB
collection.insert(doc)

## Update [single] document
let reply = collection.update(%*{
  "name": "John"
}, %*{
  "$set": {
    "surname": "Smith"
  }
})

# Check command execution status
if reply.ok:
  echo "Modified a document."

## Delete multiple documents
let removeResult = collection.remove(%*{"name": "John"})

## Check how many documents were removed
if removeResult.ok:
  echo "Removed ", removeResult.n, " documents."

## Delete single document
collection.remove(%{"name": "John"}, limit=1)

## Delete collection
collection.drop()

## Delete single document
collection.remove(%{"name": "John"}, limit=1)

## Fetch number of documents in collection
collection.count()

## Fetch number of documents in query
let tally = collection.find(%*{"name": "John"}).count()

## Fetch one document from DB returning only one field: "name".
let fetched = collection.find(%*{"name": "John"}, @["name"]).one()

## Fetch all matching documents from DB receiving seq[Bson]
let documents = collection.find(%*{"name": "John"}).all()

## Fetch all matching documents as a iterator
for document in collection.find(%*{"name": "John"}).items():
  echo document

## Force cursor to return only distinct documents by specified field.
let documents = collection.find(%*{"name": "John"}).unique("name").all()
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
  doc1 = %*{"doc1": 15}
  doc2 = %*{"doc2": "string"}

waitFor(m.insert(@[doc1, doc2]))

## Removing single document from MongoDB
waitFor(m.remove(B("doc1", 15), limit=1))

## Removing multiple documents from MongoDB
waitFor(m.remove(B("doc1", 15)))
```

Currently Supported Features
============================
Here's a list of supported features with appropriate status icons:

 * :white_check_mark: - implemented feature
 * :red_circle: - __not__ implemented feature
 * :warning: - __partly__ supported or __unstable__

BSON
----

`nimongo.bson` module implements full BSON specification, and includes means
for developer-friendly BSON creation, modification, serialization and
deserialization.

You can user either __B(...)__ template or __`%*`__ for documents creation
depending on what is more convenient for you.


```nim
let doc = B("name", "John")("surname", "Smith")("salary", 100)
let doc2 = B(
    "name", "Sam")(
    "surname", "Uncle")(
    "salary", 1000)(
    "skills", @["power", "government", "army"]
    )
```    
    
Authentication
---------------
`nimongo` supports the new SCRAM-SHA-1 challenge-response user authentication mechanism

```nim

var db: Database[Mongo]
try:
    db = newMongoDatabase("mongodb://$1:$2@localhost:27017/db" % [db_user,db_pass])
    if not db.client.authenticated: raise newException(AUTHError, "Unable to authenticate to db")
except:
    logging.error(getCurrentExceptionMsg())
    raise
    
```

MongoDB Features
----------------
This table represents MongoDB features and their implementation status within
`nimongo.mongo` Nim module.

| Block      | Feature         | Status (sync)      | Status (async)     | Notes |
|-----------:|:----------------|:------------------:|:------------------:|:------|
|Connection  |                 | __2__ / __7__      | __2__ / __7__      |       |
|            | Single server   | :white_check_mark: | :white_check_mark: |       |
|            | Replica set     | :red_circle:       | :red_circle:       |       |
|            | Socket Timeout  | :red_circle:       | :red_circle:       |       |
|            | SSL             | :red_circle:       | :red_circle:       |       |
|            | Connect Timeout | :red_circle:       | :red_circle:       | |
|            | Write Concern   | :white_check_mark: | :white_check_mark: | __setWriteConcern(...)__ |
|            | Read Preference | :red_circle:       | :red_circle:       | |
|Operations  | Insert (Single/Multiple), Remove (Single/Multiple), Update (Single/Multiple/Upsert) | :white_check_mark: | :white_check_mark: | |
|Querying    |                 | __6__ / __10__     | __5__ / __9__      | |
|            | Find one        | :white_check_mark: | :white_check_mark: | |
|            | Find all        | :white_check_mark: | :white_check_mark: | |
|            | Find iterator   | :white_check_mark: | __N/A__            | |
|            | Skip            | :white_check_mark: | :white_check_mark: | |
|            | Limit           | :white_check_mark: | :white_check_mark: | |
|            | Count           | :white_check_mark: | :white_check_mark: | |
|            | Tailable        | :red_circle:       | :red_circle:       | |
|            | Partial         | :red_circle:       | :red_circle:       | |
|            | FindAndModify   | :red_circle:       | :red_circle:       | |
|            | parallelCollectionScan | :red_circle:| :red_circle:       | |
|            | getLastError    | :white_check_mark: | :white_check_mark: | |
|Authentication |              | __1__ / __7__      | __1__ / __7__      | |
|            | authenticate    | :red_circle:       | :red_circle:       | |
|            | SCRAM-SHA-1     | :white_check_mark: | :white_check_mark: | |
|            | MONGODB-CR      | :red_circle:       | :red_circle:       | |
|            | MONGODB-X509    | :red_circle:       | :red_circle:       | |
|            | GSSAPI (Kerberos)|:red_circle:       | :red_circle:       | |
|            | PLAIN (LDAP SASL)|:red_circle:       | :red_circle:       | |
|            | logout          | :red_circle:       | :red_circle:       | |
|User Management |             | __2__ / __7__      | __2__ / __7__      | |
|            | Create User     | :white_check_mark: | :white_check_mark: | |
|            | Update User     | :red_circle:       | :red_circle:       | |
|            | Drop User       | :white_check_mark: | :white_check_mark: | |
|            | Drop all users  | :red_circle:       | :red_circle:       | |
|            | Grant roles     | :red_circle:       | :red_circle:       | |
|            | Revoke roles    | :red_circle:       | :red_circle:       | |
|            | Users info      | :red_circle:       | :red_circle:       | |
|Role Management |             | __0__ / __0__      | __0__ / __0__      | |
|Replication |                 | __1__ / __1__      | __1__ / __1__      | |
|            | Is Master       | :white_check_mark: | :white_check_mark: | |
|Sharding    |                 | __0__ / __0__      | __0__ / __0__      | |
|Instance Administration Commands|               | __6__ / __7__      | __6__ / __7__      | |
|            | Copy DB         | :red_circle:       | :red_circle:       | |
|            | List databases  | :white_check_mark: | :white_check_mark: | |
|            | Drop database   | :white_check_mark: | :white_check_mark: | |
|            | List collections| :white_check_mark: | :white_check_mark: | |
|            | Rename collection|:white_check_mark: | :white_check_mark: | |
|            | Drop collection | :white_check_mark: | :white_check_mark: | |
|            | Create collection|:white_check_mark: | :white_check_mark: | |
|Diagnostic  |                 | __0__ / __0__      | __0__ / __0__      | |
|GridFS      |                 | __0__ / __0__      | __0__ / __0__      | |
|Indices     |                 | __0__ / __4__      | __0__ / __4__      | |
|            | Create Index    | :red_circle:       | :red_circle:       | |
|            | Drop Index      | :red_circle:       | :red_circle:       | |
|            | Drop Indices    | :red_circle:       | :red_circle:       | |
|            | Ensure Index    | :red_circle:       | :red_circle:       | |
|Aggregation |                 | __2__ / __5__      | __2__ / __5__      | |
|            | aggregate       | :red_circle:       | :red_circle:       | |
|            | count           | :white_check_mark: | :white_check_mark: | |
|            | distinct        | :white_check_mark: | :white_check_mark: | __Cursor.unique__ proc |
|            | group           | :red_circle:       | :red_circle:       | |
|            | mapReduce       | :red_circle:       | :red_circle:       | |
|Geospatial  |                 | __0__ /__3__       | __0__ / __3__      | |
|            | geoNear         | :red_circle:       | :red_circle:       | |
|            | geoSearch       | :red_circle:       | :red_circle:       | |
|            | geoWalk         | :red_circle:       | :red_circle:       | |
|Auditing    |                 | __0__ / __1__      | __0__ / __1__      | |
|            |logApplicationMessage|:red_circle:    | :red_circle:       | |

__P.S.__ Contribution is welcomed :)
