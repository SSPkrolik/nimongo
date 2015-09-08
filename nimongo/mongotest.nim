import oids
import unittest

import nimongo.bson
import nimongo.mongo

suite "Mongo client test suite":

    setup:
        var
            m: Mongo = newMongo()
        let
            db: Database = m["db"]
            c: Collection = db["collection"]

        require(m.connect() == true)

    test "Mongo object `$` operator":
        check($m == "mongodb://127.0.0.1:27017")

    test "Taking database":
        check($db == "db")

    test "Taking collection":
        check($c == "db.collection")

    test "Inserting single document":
        check(c.insert(B("double", 3.1415)) == true)

    test "Inserting multiple documents":
        let
            doc1 = B("integer", 100'i32)
            doc2 = B("string", "hello")("subdoc", B("name", "John"))
            doc3 = B("array", @["element1", "element2", "element3"])
        check(c.insert(@[doc1, doc2, doc3]) == true)

    test "Update single document":
        let
            selector = B("integer", 100'i32)
            updater  = B("$set", B("integer", 200))

        check(c.update(selector, updater) == true)

    test "Remove document":
        let
            doc = B("string", "hello")
        check(c.remove(doc) == true)

    test "Query single document":
        let myId = genOid()
        let doc = B("string", "somedoc")("myid", myId)

        check(c.insert(doc))

        let res = c.find(B("myid", myId)).one()
        let myIdFromDb: Oid = res["myid"]
        check(myIdFromDb == myId)

    test "Query multiple documents as a sequence":
        let doc = B("string", "hello")
        check(c.insert(doc))
        check(c.insert(doc))
        let docs = c.find(B("string", "hello")).all()
        check(docs.len() > 1)

    test "Query multiple documents as iterator":
        let doc = B("string", "hello")
        check(c.insert(doc))
        check(c.insert(doc))
        let request = c.find(B("string", "hello"))
        for document in request.items():
            check(document["string"] == "hello")
