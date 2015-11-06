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
require(sm.connect() == true)
require(waitFor(am.connect()) == true)

suite "Mongo instance administration commands test suite":

  echo "\n Mongo instance administration commands test suite\n"

  setup:
    discard

  test "[ASYNC] [SYNC] Command: 'isMaster'":
    var m: bool
    m = sm.isMaster()
    check(m == true or m == false)
    m = waitFor(am.isMaster())
    check(m == true or m == false)

  test "[ASYNC] [SYNC] Command: 'dropDatabase'":
    check(sdb.drop() == true)
    check(waitFor(adb.drop()) == true)

  test "[ASYNC] [SYNC] Command: 'listDatabases'":
    sco.insert(B("test", "test"))
    check("testdb" in sm.listDatabases())
    check("testdb" in waitFor(am.listDatabases()))
    sco.remove(B("test", "test"), RemoveSingle)

  #test "[ASYNC] [SYNC] Command: 'listCollections'":
  #  let sclist = sdb.listCollections()
  #  #check(sclist.len() == 3)

suite "Mongo connection error-handling operations":

  echo "\n Mongo connection error-handling operations\n"

  setup:
    discard

  test "[ASYNC] [SYNC] Command: 'getLastError'":
    check(sm.getLastError().ok == true)
    check(waitFor(am.getLastError()).ok == true)

suite "Authentication":

  echo "\nAuthentication\n"

  setup:
    discard

  test "[     ] [SYNC] Command: 'authenticate', method: 'plain'":
    let authtestdb = newMongoDatabase("mongodb://test:test@localhost:8081/testdb")
    check($authtestdb == "testdb")
    authtestdb[TestSyncCol].insert(B("data", "auth"))

suite "Mongo collection-level operations":

  echo "\n Mongo collection-level operations\n"

  setup:
    discard sco.drop()
    discard waitFor(aco.drop())

  test "[ASYNC] [SYNC] 'count' documents in collection":

    check(sco.insert(
      @[
        B("iter", 0.int32)("label", "l"),
        B("iter", 1.int32)("label", "l"),
        B("iter", 2.int32)("label", "l"),
        B("iter", 3.int32)("label", "l"),
        B("iter", 4.int32)("label", "l")
      ]
    ))
    check(sco.count() == 5)

    check(waitFor(aco.insert(
      @[
        B("iter", 0.int32)("label", "l"),
        B("iter", 1.int32)("label", "l"),
        B("iter", 2.int32)("label", "l"),
        B("iter", 3.int32)("label", "l"),
        B("iter", 4.int32)("label", "l")
      ]
    )))
    check(waitFor(aco.count()) == 5)

  test "[ASYNC] [SYNC] 'drop' collection":
    check(sco.insert(B("svalue", "hello")) == true)
    discard sco.drop()
    check(sco.find(B("svalue", "hello")).all().len() == 0)

    check(waitFor(aco.insert(B("svalue", "hello"))) == true)
    discard waitFor(aco.drop())
    check(waitFor(aco.find(B("svalue", "hello")).all()).len() == 0)


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
    check(sco.insert(B("double", 3.1415)) == true)
    check(waitFor(aco.insert(B("double", 3.1415))) == true)

  test "[ASYNC] [SYNC] Inserting multiple documents":
    let
      doc1 = B("integer", 100'i32)
      doc2 = B("string", "hello")("subdoc", B("name", "John"))
      doc3 = B("array", @["element1", "element2", "element3"])

    check(sco.insert(@[doc1, doc2, doc3]) == true)
    check(waitFor(aco.insert(@[doc1, doc2, doc3])) == true)

  test "[ASYNC] [SYNC] Update single document":
    let
      selector = B("integer", 100'i32)
      updater  = B("$set", B("integer", 200'i32))

    check(sco.insert(@[selector, selector]) == true)
    check(waitFor(aco.insert(@[selector, selector])) == true)

    check(sco.update(selector, updater, UpdateSingle, NoUpsert) == true)
    check(waitFor(aco.update(selector, updater, UpdateSingle, NoUpsert)) == true)

    check(sco.find(B("integer", 200)).all().len() == 1)
    check(waitFor(aco.find(B("integer", 200)).all()).len() == 1)

  test "[ASYNC] [SYNC] Update multiple documents":
    let
      selector = B("integer", 100'i32)
      updater  = B("$set", B("integer", 200'i32))

    check(sco.insert(@[selector, selector]) == true)
    check(waitFor(aco.insert(@[selector, selector])) == true)

    check(sco.update(selector, updater, UpdateMultiple, NoUpsert) == true)
    check(waitFor(aco.update(selector, updater, UpdateMultiple, NoUpsert)) == true)

    check(sco.find(B("integer", 200'i32)).all().len() == 2)
    check(waitFor(aco.find(B("integer", 200'i32)).all()).len() == 2)

  test "[ASYNC] [SYNC] Upsert":
    let
      selector = B("integer", 100'i64)
      updater  = B("$set", B("integer", 200'i64))

    check(sco.update(selector, updater, UpdateSingle, Upsert) == true)
    check(waitFor(aco.update(selector, updater, UpdateSingle, Upsert)) == true)

    check(sco.find(B("integer", 200)).all().len() == 1)
    check(waitFor(aco.find(B("integer", 200)).all()).len() == 1)

  test "[ASYNC] [SYNC] Remove single document":
    let doc = B("string", "hello")
    check(sco.insert(doc) == true)
    check(sco.remove(doc, RemoveSingle) == true)
    check(waitFor(aco.insert(doc)) == true)
    check(waitFor(aco.remove(doc, RemoveSingle)) == true)

  test "[ASYNC] [SYNC] Remove multiple documents":
    check(sco.insert(@[B("string", "value"), B("string", "value")]) == true)
    check(sco.remove(B("string", "value"), RemoveMultiple))
    check(sco.find(B("string", "value")).all().len() == 0)

    check(waitFor(aco.insert(@[B("string", "value"), B("string", "value")])) == true)
    waitFor(sleepAsync(1))
    check(waitFor(aco.remove(B("string", "value"), RemoveMultiple)) == true)
    check(waitFor(aco.find(B("string", "value")).all()).len() == 0)

suite "Mongo aggregation commands":

  echo "\n Mongo aggregation commands\n"

  setup:
    discard sco.drop()
    discard waitFor(aco.drop())

  test "[ASYNC] [SYNC] Count documents in query result":
    sco.insert(@[B("string", "value"), B("string", "value")])
    check(sco.find(B("string", "value")).count() == 2)

    check(waitFor(aco.insert(@[B("string", "value"), B("string", "value")])) == true)
    check(waitFor(aco.find(B("string", "value")).count()) == 2)

  test "[ASYNC] [SYNC] Query distinct values by field in collection documents":
    sco.insert(@[B("string", "value")("int", 1'i64), B("string", "value")("double", 2.0)])
    check(sco.find(B("string", "value")).unique("string") == @["value"])

    check(waitFor(aco.insert(@[B("string", "value")("int", 1'i64), B("string", "value")("double", 2.0)])) == true)
    check(waitFor(aco.find(B("string", "value")).unique("string")) == @["value"])

suite "Mongo client querying test suite":

  echo "\n Mongo client querying\n"

  setup:
    discard sco.drop()
    discard waitFor(aco.drop())

  test "[ASYNC] [SYNC] Query single document":
    let myId = genOid()
    check(sco.insert(B("string", "somedoc")("myid", myId)))
    check(sco.find(B("myid", myId)).one()["myid"] == myId)

    check(waitFor(aco.insert(B("string", "somedoc")("myid", myId))) == true)
    check(waitFor(aco.find(B("myid", myId)).one())["myid"] == myId)

  test "[ASYNC] [SYNC] Query multiple documents as a sequence":
    check(sco.insert(@[B("string", "value"), B("string", "value")]) == true)
    check(sco.find(B("string", "value")).all().len() == 2)

    check(waitFor(aco.insert(@[B("string", "value"), B("string", "value")])) == true)
    check(waitFor(aco.find(B("string", "value")).all()).len() == 2)

  test "[ N/A ] [SYNC] Query multiple documents as iterator":
    check(sco.insert(B("string", "hello")))
    check(sco.insert(B("string", "hello")))
    for document in sco.find(B("string", "hello")).items():
      check(document["string"] == "hello")

  test "[ASYNC] [SYNC] Query multiple documents up to limit":
    check(sco.insert(
      @[
        B("iter", 0.int32)("label", "l"),
        B("iter", 1.int32)("label", "l"),
        B("iter", 2.int32)("label", "l"),
        B("iter", 3.int32)("label", "l"),
        B("iter", 4.int32)("label", "l")
      ]
    ))
    check(sco.find(B("label", "l")).limit(3).all().len() == 3)

    check(waitFor(aco.insert(
      @[
        B("iter", 0.int32)("label", "l"),
        B("iter", 1.int32)("label", "l"),
        B("iter", 2.int32)("label", "l"),
        B("iter", 3.int32)("label", "l"),
        B("iter", 4.int32)("label", "l"),
      ]
    )))
    check(waitFor(aco.find(B("label", "l")).limit(3).all()).len() == 3)

  test "[ASYNC] [SYNC] Skip documents":
    check(sco.insert(
      @[
        B("iter", 0.int32)("label", "l"),
        B("iter", 1.int32)("label", "l"),
        B("iter", 2.int32)("label", "l"),
        B("iter", 3.int32)("label", "l"),
        B("iter", 4.int32)("label", "l")
      ]
    ))
    check(sco.find(B("label", "l")).skip(3).all().len() == 2)

    check(waitFor(aco.insert(
      @[
        B("iter", 0.int32)("label", "l"),
        B("iter", 1.int32)("label", "l"),
        B("iter", 2.int32)("label", "l"),
        B("iter", 3.int32)("label", "l"),
        B("iter", 4.int32)("label", "l")
      ]
    )))
    check(waitFor(aco.find(B("label", "l")).skip(3).all()).len() == 2)

echo ""

# Collections must not exist before tests in the suite
discard sco.drop()
discard waitFor(aco.drop())
