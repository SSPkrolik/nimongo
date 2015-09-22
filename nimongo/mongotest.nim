import asyncdispatch
import oids
import unittest

import bson
import mongo

const
  TestDB       = "testdb"
  TestSyncCol  = "sync"
  TestAsyncCol = "async"

suite "Mongo instance administration commands test suite":

  echo "\n Mongo instance administration commands test suite\n"

  setup:
    var
      sm: Mongo = newMongo()           ## Mongo synchronous client
      am: AsyncMongo = newAsyncMongo() ## Mongo asynchronous client

    # Connection is required for running tests
    require(sm.connect() == true)
    require(waitFor(am.connect()) == true)

    # Collections must not exist before tests in the suite
    discard sm[TestDB][TestSyncCol].drop()
    discard waitFor(am[TestDB][TestAsyncCol].drop())

  test "[ASYNC] [SYNC] Command: 'isMaster'":
    var m: bool
    m = sm.isMaster()
    check(m == true or m == false)
    m = waitFor(am.isMaster())
    check(m == true or m == false)

suite "Mongo client operations test suite":

  echo "\n Mongo client operations \n"

  setup:
    var
        sm: Mongo = newMongo()           ## Mongo synchronous client
        am: AsyncMongo = newAsyncMongo() ## Mongo asynchronous client
    let
        sdb: Database[Mongo] = sm[TestDB]
        adb: Database[AsyncMongo] = am[TestDB]
        sco: Collection[Mongo] = sdb[TestSyncCol]
        aco: Collection[AsyncMongo] = adb[TestAsyncCol]

    require(sm.connect() == true)
    require(waitFor(am.connect()) == true)

    # Collections must not exist before tests in the suite
    discard sm[TestDB][TestSyncCol].drop()
    discard waitFor(am[TestDB][TestAsyncCol].drop())

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

    check(sco.find(B("integer", 200)).all().len() == 2)
    check(waitFor(aco.find(B("integer", 200)).all()).len() == 2)

  test "[ASYNC] [SYNC] Upsert":
    let
      selector = B("integer", 100'i32)
      updater  = B("$set", B("integer", 200'i32))

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
    check(waitFor(aco.remove(B("string", "value"), RemoveMultiple)) == true)
    check(waitFor(aco.find(B("string", "value")).all()).len() == 0)

suite "Mongo client querying test suite":

  echo "\n Mongo client querying\n"

  setup:
    var
        sm: Mongo = newMongo()           ## Mongo synchronous client
        am: AsyncMongo = newAsyncMongo() ## Mongo asynchronous client
    let
        sdb: Database[Mongo] = sm[TestDB]
        adb: Database[AsyncMongo] = am[TestDB]
        sco: Collection[Mongo] = sdb[TestSyncCol]
        aco: Collection[AsyncMongo] = adb[TestAsyncCol]

    require(sm.connect() == true)
    require(waitFor(am.connect()) == true)

    # Collections must not exist before tests in the suite
    discard sm[TestDB][TestSyncCol].drop()
    discard waitFor(am[TestDB][TestAsyncCol].drop())

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
    for i in 0..<5: check(sco.insert(B("iter", i.int32)("label", "l")))
    check(sco.find(B("label", "l")).limit(3).all().len() == 3)

    for i in 0..<5: check(waitFor(aco.insert(B("iter", i.int32)("label", "l"))))
    check(waitFor(aco.find(B("label", "l")).limit(3).all()).len() == 3)

  test "[ASYNC] [SYNC] Skip documents":
    for i in 0..<5: check(sco.insert(B("iter", i.int32)("label", "l")))
    check(sco.find(B("label", "l")).skip(3).all().len() == 2)

    for i in 0..<5: check(waitFor(aco.insert(B("iter", i.int32)("label", "l"))))
    check(waitFor(aco.find(B("label", "l")).skip(3).all()).len() == 2)

echo ""
