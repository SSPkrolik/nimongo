import asyncdispatch
import oids
import strutils
import times
import unittest

import bson
import mongo
import timeit

const
  TestDB       = "testdb"
  TestSyncCol  = "sync"
  TestAsyncCol = "async"

var
  sm: Mongo = newMongo()           ## Mongo synchronous client
  am: AsyncMongo = newAsyncMongo() ## Mongo asynchronous client

let
  sdb: Database[Mongo] = sm[TestDB]
  adb: Database[AsyncMongo] = am[TestDB]
  sco: Collection[Mongo] = sdb[TestSyncCol]
  aco: Collection[AsyncMongo] = adb[TestAsyncCol]

# Connection is required for running tests
require(sm.connect())
require(waitFor(am.connect()))

suite "Mongo instance administration commands test suite":

  echo "\n Mongo instance administration commands test suite\n"

  setup:
    discard

  test "[ASYNC] [SYNC] Command: 'isMaster'":
    var m: bool
    m = sm.isMaster()
    m = waitFor(am.isMaster())

  test "[ASYNC] [SYNC] Command: 'dropDatabase'":
    check(sdb.drop())
    check(waitFor(adb.drop()))

  test "[ASYNC] [SYNC] Command: 'listDatabases'":
    sco.insert(%*{"test": "test"})
    check("testdb" in sm.listDatabases())
    check("testdb" in waitFor(am.listDatabases()))
    sco.remove(%*{"test": "test"}, limit=1)

  test "[     ] [SYNC] Command: 'listCollections'":
    let sclist = sdb.listCollections()
    check(sclist.len() == 2)

    let aclist = waitFor(adb.listCollections())
    check(aclist.len() == 2)

suite "Mongo connection error-handling operations":

  echo "\n Mongo connection error-handling operations\n"

  setup:
    discard

  test "[ASYNC] [SYNC] Command: 'getLastError'":
    check(sm.getLastError().ok)
    check(waitFor(am.getLastError()).ok)

suite "Authentication":

  echo "\n Authentication\n"

  setup:
    discard

  test "[     ] [SYNC] Command: 'authenticate', method: 'SCRAM-SHA-1'":
    check(sdb.createUser("test", "test"))
    let authtestdb = newMongoDatabase("mongodb://test:test@localhost:27017/testdb")
    check($authtestdb == "testdb")
    authtestdb[TestSyncCol].insert(%*{"data": "auth"})
    check(sdb.dropUser("test"))

suite "User Management":

  echo "\n User management\n"

  setup:
    discard

  test "[ASYNC][SYNC] Command: 'createUser' without roles and custom data":
    check(sdb.createUser("testuser", "testpass"))
    check(waitFor(adb.createUser("testuser2", "testpass2")))
    check(sdb.dropUser("testuser"))
    check(waitFor(adb.dropUser("testuser2")))

  test "[ASYNC][SYNC] Command: 'dropUser'":
    check(sdb.createUser("testuser2", "testpass2"))
    check(sdb.dropUser("testuser2"))
    check(waitFor(adb.createUser("testuser2", "testpass2")))
    check(waitFor(adb.dropUser("testuser2")))

suite "Mongo collection-level operations":

  echo "\n Mongo collection-level operations\n"

  setup:
    discard sco.drop()
    discard waitFor(aco.drop())

  test "[ASYNC] [SYNC] 'count' documents in collection":

    check(sco.insert(
      @[
        %*{"iter": 0.int32, "label": "l"},
        %*{"iter": 1.int32, "label": "l"},
        %*{"iter": 2.int32, "label": "l"},
        %*{"iter": 3.int32, "label": "l"},
        %*{"iter": 4.int32, "label": "l"},
      ]
    ))
    check(sco.count() == 5)

    check(waitFor(aco.insert(
      @[
        %*{"iter": 0.int32, "label": "l"},
        %*{"iter": 1.int32, "label": "l"},
        %*{"iter": 2.int32, "label": "l"},
        %*{"iter": 3.int32, "label": "l"},
        %*{"iter": 4.int32, "label": "l"},
      ]
    )))
    check(waitFor(aco.count()) == 5)

  test "[ASYNC] [SYNC] 'drop' collection":
    check(sco.insert(%*{"svalue": "hello"}))
    discard sco.drop()
    check(sco.find(%*{"svalue": "hello"}).all().len() == 0)

    check(waitFor(aco.insert(%*{"svalue": "hello"})))
    discard waitFor(aco.drop())
    check(waitFor(aco.find(%*{"svalue": "hello"}).all()).len() == 0)


suite "Mongo client operations test suite":

  echo "\n Mongo client operations \n"

  setup:
    discard sco.drop()
    discard waitFor(aco.drop())

  test "[ASYNC] [SYNC] Mongo object `$` operator":
    check($sm == "mongodb://127.0.0.1:27017")
    check($am == "mongodb://127.0.0.1:27017")

  test "[ASYNC] [SYNC] Taking database":
    check($sdb == "testdb")
    check($adb == "testdb")

  test "[ASYNC] [SYNC] Taking collection":
    check($sco == "testdb.sync")
    check($aco == "testdb.async")

  test "[ASYNC] [SYNC] Inserting single document":
    check(sco.insert(%*{"double": 3.1415}))
    check(waitFor(aco.insert(%*{"double": 3.1415})))

  test "[ASYNC] [SYNC] Inserting multiple documents":
    let
      doc1 = %*{"integer": 100'i32}
      doc2 = %*{"string": "hello", "subdoc": {"name": "John"}}
      doc3 = %*{"array": ["element1", "element2", "element3"]}

    check(sco.insert(@[doc1, doc2, doc3]))
    check(waitFor(aco.insert(@[doc1, doc2, doc3])))

  test "[ASYNC] [SYNC] Update single document":
    let
      selector = %*{"integer": "integer"}
      updater  = %*{"$set": {"integer": "string"}}

    check(sco.insert(@[selector, selector]))
    check(waitFor(aco.insert(@[selector, selector])))

    check(sco.update(selector, updater, false, false))
    check(waitFor(aco.update(selector, updater, false, false)))

    check(sco.find(%*{"integer": "string"}).all().len() == 1)
    check(waitFor(aco.find(%*{"integer": "string"}).all()).len() == 1)

  test "[ASYNC] [SYNC] Update multiple documents":
    let
      selector = %*{"integer": 100'i32}
      updater  = %*{"$set": {"integer": 200'i32}}

    check(sco.insert(@[selector, selector]))
    check(waitFor(aco.insert(@[selector, selector])))

    check(sco.update(selector, updater, true, false))
    check(waitFor(aco.update(selector, updater, true, false)))

    check(sco.find(%*{"integer": 200'i32}).all().len() == 2)
    check(waitFor(aco.find(%*{"integer": 200'i32}).all()).len() == 2)

  test "[ASYNC] [SYNC] Upsert":
    let
      selector = %*{"integer": 100'i64}
      updater  = %*{"$set": {"integer": 200'i64}}

    check(sco.update(selector, updater, false, true))
    check(waitFor(aco.update(selector, updater, false, true)))

    check(sco.find(%*{"integer": 200}).all().len() == 1)
    check(waitFor(aco.find(%*{"integer": 200}).all()).len() == 1)

  test "[ASYNC] [SYNC] Remove single document":
    let doc = %*{"string": "hello"}
    check(sco.insert(doc))
    check(sco.remove(doc, limit=1).ok)
    check(waitFor(aco.insert(doc)))
    check(waitFor(aco.remove(doc, limit=1)).ok)

  test "[ASYNC] [SYNC] Remove multiple documents":
    check(sco.insert(@[%*{"string": "value"}, %*{"string": "value"}]))
    check(sco.remove(%*{"string": "value"}).ok)
    check(sco.find(%*{"string": "value"}).all().len() == 0)

    check(waitFor(aco.insert(@[%*{"string": "value"}, %*{"string": "value"}])))
    check(waitFor(aco.remove(%*{"string": "value"})).ok)
    check(waitFor(aco.find(%*{"string": "value"}).all()).len() == 0)

suite "Mongo aggregation commands":

  echo "\n Mongo aggregation commands\n"

  setup:
    discard sco.drop()
    discard waitFor(aco.drop())

  test "[ASYNC] [SYNC] Count documents in query result":
    sco.insert(@[%*{"string": "value"}, %*{"string": "value"}])
    check(sco.find(%*{"string": "value"}).count() == 2)

    check(waitFor(aco.insert(@[%*{"string": "value"}, %*{"string": "value"}])))
    check(waitFor(aco.find(%*{"string": "value"}).count()) == 2)

  test "[ASYNC] [SYNC] Query distinct values by field in collection documents":
    sco.insert(@[%*{"string": "value", "int": 1'i64}, %*{"string": "value", "double": 2.0}])
    check(sco.find(%*{"string": "value"}).unique("string") == @["value"])

    check(waitFor(aco.insert(@[%*{"string": "value", "int": 1'i64}, %*{"string": "value", "double": 2.0}])))
    check(waitFor(aco.find(%*{"string": "value"}).unique("string")) == @["value"])

suite "Mongo client querying test suite":

  echo "\n Mongo client querying\n"

  setup:
    discard sco.drop()
    discard waitFor(aco.drop())

  test "[ASYNC] [SYNC] Query single document":
    let myId = genOid()
    check(sco.insert(%*{"string": "somedoc", "myid": myId}))
    check(sco.find(%*{"myid": myId}).one()["myid"] == myId)

    check(waitFor(aco.insert(%*{"string": "somedoc", "myid": myId})))
    check(waitFor(aco.find(%*{"myid": myId}).one())["myid"] == myId)

  test "[ASYNC] [SYNC] Query multiple documents as a sequence":
    check(sco.insert(@[%*{"string": "value"}, %*{"string": "value"}]))
    check(sco.find(%*{"string": "value"}).all().len() == 2)

    check(waitFor(aco.insert(@[%*{"string": "value"}, %*{"string": "value"}])))
    check(waitFor(aco.find(%*{"string": "value"}).all()).len() == 2)

  test "[ N/A ] [SYNC] Query multiple documents as iterator":
    check(sco.insert(%*{"string": "hello"}))
    check(sco.insert(%*{"string": "hello"}))
    for document in sco.find(%*{"string": "hello"}).items():
      check(document["string"] == "hello")

  test "[ASYNC] [SYNC] Query multiple documents up to limit":
    check(sco.insert(
      @[
        %*{"iter": 0.int32, "label": "l"},
        %*{"iter": 1.int32, "label": "l"},
        %*{"iter": 2.int32, "label": "l"},
        %*{"iter": 3.int32, "label": "l"},
        %*{"iter": 4.int32, "label": "l"}
      ]
    ))
    check(sco.find(%*{"label": "l"}).limit(3).all().len() == 3)

    check(waitFor(aco.insert(
      @[
        %*{"iter": 0.int32, "label": "l"},
        %*{"iter": 1.int32, "label": "l"},
        %*{"iter": 2.int32, "label": "l"},
        %*{"iter": 3.int32, "label": "l"},
        %*{"iter": 4.int32, "label": "l"}
      ]
    )))
    check(waitFor(aco.find(%*{"label": "l"}).limit(3).all()).len() == 3)

  test "[ASYNC] [SYNC] Skip documents":
    check(sco.insert(
      @[
        %*{"iter": 0.int32, "label": "l"},
        %*{"iter": 1.int32, "label": "l"},
        %*{"iter": 2.int32, "label": "l"},
        %*{"iter": 3.int32, "label": "l"},
        %*{"iter": 4.int32, "label": "l"},
      ]
    ))
    check(sco.find(%*{"label": "l"}).skip(3).all().len() == 2)

    check(waitFor(aco.insert(
      @[
        %*{"iter": 0.int32, "label": "l"},
        %*{"iter": 1.int32, "label": "l"},
        %*{"iter": 2.int32, "label": "l"},
        %*{"iter": 3.int32, "label": "l"},
        %*{"iter": 4.int32, "label": "l"},
      ]
    )))
    check(waitFor(aco.find(%*{"label": "l"}).skip(3).all()).len() == 2)

echo ""

# Collections must not exist before tests in the suite
discard sco.drop()
discard waitFor(aco.drop())
