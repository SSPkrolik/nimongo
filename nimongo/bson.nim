import json
import oids
import sequtils
import tables
import streams

type
    BsonSubtype* = distinct byte

    BsonKind* = enum
        BsonKindStringUTF8      = 0x02.char

discard """
    BsonKindDouble          = BsonType(0x01)
    BEDocument        = BsonType(0x03)
    BEArray           = BsonType(0x04)
    BEBinary          = BsonType(0x05)
    BEUndefined       = BsonType(0x06)
    BEOid             = BsonType(0x07)
    BEBool            = BsonType(0x08)
    BEDateTimeUTC     = BsonType(0x09)
    BENull            = BsonType(0x0A)
    BERegexp          = BsonType(0x0B)
    BEDBPointer       = BsonType(0x0C)
    BEJSCode          = BsonType(0x0D)
    BEDeprecated      = BsonType(0x0E)
    BEJSCodeWithScope = BsonType(0x0F)
    BEInt32           = BsonType(0x10)
    BETimestamp       = BsonType(0x11)
    BEInt64           = BsonType(0x12)
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

type BsonString* = object of Bson

proc `$`*(bs: BsonString): string =
    return $bs.data

type BsonDocument* = ref object of RootObj
    size: int32
    elements: OrderedTableRef[string, Bson]

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
    let a = cast[array[0..3, char]](bs.size)
    result = ""
    for c in a:
        result = result & c
    for key, val in bs.elements.pairs():
        result = result & key & char(0) & val
    result = result & char(0)

converter toBson(s: string): Bson =
    result.data = BsonKindStringUTF8 & s

when isMainModule:
    echo "Testing nimongo/bson.nim module..."
    var testdoc = newBsonDocument()
    testdoc["name"] = "John!"
