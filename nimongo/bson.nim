import json
import oids
import sequtils
import tables
import streams

type
    BsonSubtype* = distinct byte

    BsonKind* = enum
        BsonKindDouble          = 0x01.char
        BsonKindStringUTF8      = 0x02.char
        BsonKindDocument        = 0x03.char
        BsonKindArray           = 0x04.char
        BsonKindBool            = 0x08.char
        BsonKindNull            = 0x0A.char
        BsonKindInt64           = 0x12.char

discard """
    BEBinary          = BsonType(0x05)
    BEUndefined       = BsonType(0x06)
    BEOid             = BsonType(0x07)
    BEDateTimeUTC     = BsonType(0x09)
    BERegexp          = BsonType(0x0B)
    BEDBPointer       = BsonType(0x0C)
    BEJSCode          = BsonType(0x0D)
    BEDeprecated      = BsonType(0x0E)
    BEJSCodeWithScope = BsonType(0x0F)
    BEInt32           = BsonType(0x10)
    BETimestamp       = BsonType(0x11)
    BEMinimumKey      = BsonType(0xFF)
    BEMaximumKey      = BsonType(0x7F)
"""

discard """
  BsonSubtypeKind* = enum
    BSGeneric         = BsonSubtype(0x00)
    BSFunction        = BsonSubtype(0x01)
    BSBinaryOld       = BsonSubtype(0x02)
    BSUUIDOld         = BsonSubtype(0x03)
    BSUUID            = BsonSubtype(0x04)
    BSMD5             = BsonSubtype(0x05)
    BSUserDefined     = BsonSubtype(0x80)

"""

converter toChar*(bk: BsonKind): char = bk.char

type Bson* = object of RootObj
    data*: string

converter toString(bs: Bson): string =
    return bs.data

type BsonString = object of Bson
type BsonNull = object of Bson

proc `$`*(bs: BsonString): string =
    return $bs.data

type BsonDocument* = ref object of RootObj
    size: int32
    data: string
    elements: OrderedTableRef[string, Bson]

proc len*(bs: BsonDocument): int =
    return int(bs.size)

proc newBsonDocument*(): BsonDocument =
    ## Create empty BSON document
    result.new
    result.size = 5
    result.elements = newOrderedTable[string, Bson](0)

proc `[]=`*(bs: BsonDocument, key: string, val: Bson) =
    ## Add field to BSON document
    inc(bs.size, key.len + val.data.len + 1)
    bs.elements[key] = val

proc `[]`*(bs: BsonDocument, key: string): Bson =
    ## Retrieve field from BSON document
    return bs.elements[key]

proc `$`*(bs: BsonDocument): string =
    ## Serialize Bson document into byte stream
    discard """
    let a = cast[array[0..3, char]](bs.size)
    result = ""
    for c in a:
        result = result & c
    for key, val in bs.elements.pairs():
        result = result & key & char(0) & val
    result = result & char(0)
    """
    return bs.data

converter toBson(s: string): Bson =
    result.data = BsonKindStringUTF8 & s

proc jsonToBson*(j: JsonNode): BsonDocument =

    var finalDoc: BsonDocument = newBsonDocument()

    proc simpleConv(j: JsonNode, name: string = ""): string =
        if j.kind == JNull:
            inc(finalDoc.size, 2 + name.len)
            return BsonKindNull & name & char(0)
        elif j.kind == JBool:
            inc(finalDoc.size, 2 + name.len)
            return BsonKindBool & name & char(0) & (if j.bval: 1.char else: 0.char)
        elif j.kind == JInt:
            let
                num: int64 = int64(j.num)
                numarr = cast[array[0..7, char]](num)
            var res: string
            for c in numarr:
                res = res & c
            inc(finalDoc.size, 2 + 8 + name.len)
            return BsonKindInt64 & name & char(0) & res
        elif j.kind == JFloat:
            let
                fnum = float64(j.fnum)
                numarr = cast[array[0..7, char]](fnum)
            var res: string
            for c in numarr:
                res = res & c
            inc(finalDoc.size, 2 + 8 + name.len)
            return BsonKindDouble & name & char(0) & res
        elif j.kind == JString:
            inc(finalDoc.size, 3 + name.len + j.str.len)
            return BsonKindStringUTF8 & name & char(0) & j.str & char(0)
        elif j.kind == JObject:
            for key, jVal in j.pairs():
                return simpleConv(jVal, key)
        #elif j.kind == JArray:
        #    for i, jVal in j:
        #        return simpleConv(j, key)

    finalDoc.data = simpleConv(j)
    return finalDoc

when isMainModule:
    echo "Testing nimongo/bson.nim module..."
    var testdoc = newBsonDocument()
    testdoc["name"] = "John!"
