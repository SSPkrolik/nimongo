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

suite "Mongo client test suite":

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

  discard """
  test "[ sync] Update single document":
      let
          selector = B("integer", 100'i32)
          updater  = B("$set", B("integer", 200))

      check(c.update(selector, updater) == true)

  test "[ sync] Remove document":
      let
          doc = B("string", "hello")
      check(c.remove(doc) == true)

  test "[ sync] Query single document":
      let myId = genOid()
      let doc = B("string", "somedoc")("myid", myId)

      check(c.insert(doc))

      let res = c.find(B("myid", myId)).one()
      let myIdFromDb: Oid = res["myid"]
      check(myIdFromDb == myId)

  test "[ sync] Query multiple documents as a sequence":
      let doc = B("string", "hello")
      check(c.insert(doc))
      check(c.insert(doc))
      let docs = c.find(B("string", "hello")).all()
      check(docs.len() > 1)

  test "[ sync] Query multiple documents as iterator":
      let doc = B("string", "hello")
      check(c.insert(doc))
      check(c.insert(doc))
      let request = c.find(B("string", "hello"))
      for document in request.items():
          check(document["string"] == "hello")
  """

discard """

suite "Mongo async client test suite":

  setup:
    let
      a: Mongo = newAsyncMongo()
      db: Database = a["db"]
      c: Collection = db["async"]
      connected = waitFor(a.asyncConnect())
    require(connected == true)

  test "[async] Insert single document":
    let insertResult = waitFor(c.asyncInsert(B("hello", "async")))
    check(insertResult == true)

  test "[async] Insert multiple documents":
    let
      d1 = B("doc1", "string")
      d2 = B("doc2", "string")
      insertResult = waitFor(c.asyncInsert(@[d1, d2]))
    check(insertResult == true)

  test "[async] Remove single document":
    let removeResult = waitFor(c.asyncRemove(B("doc1", "string")))
    check(removeResult == true)

"""
