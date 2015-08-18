import json
import oids
import sequtils
import tables

discard """

type
  BsonType* = distinct byte
  BsonSubtype* = distinct byte

  BsonTypeKind* = enum
    BEDouble          = BsonType(0x01)
    BEStringUTF8      = BsonType(0x02)
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

  BsonSubtypeKind* = enum
    BSGeneric         = BsonSubtype(0x00)
    BSFunction        = BsonSubtype(0x01)
    BSBinaryOld       = BsonSubtype(0x02)
    BSUUIDOld         = BsonSubtype(0x03)
    BSUUID            = BsonSubtype(0x04)
    BSMD5             = BsonSubtype(0x05)
    BSUserDefined     = BsonSubtype(0x80)

"""

type Bson = object of RootObj

type BsonString* = object of Bson
    data: seq[byte]

type BsonDocument* = object of Bson
    size: int32
    data: Bson

proc `$`*(bs: BsonString): string =
    discard

proc `$`*(bs: BsonDocument): string =
    discard

converter toBsonString*(s: string): BsonString =
    ## Implicitly converts string to its BSON representation
    result.data = newSeq[byte](s.len + 1)
    result.data[0] = 0x02
    for i in 1 ..< s.len:
        result.data[i] = s[i].byte

discard """
proc newBObject*(): Bson =
  result.new
  result.kind = BObject
  result.obj = newTable[string, Bson](initialSize=4)

proc newBArray*(): Bson =
  result.new
  result.kind = BArray
  result.arr = @[]

proc newBString*(): Bson =
  result.new
  result.kind = BString
  result.str = ""

proc newBNumber*(): Bson =
  result.new
  result.kind = BNumber
  result.num = 0.0

proc newBBool*(): Bson =
  result.new
  result.kind = BBool
  result.yes = false

proc newBNull*(): Bson =
  result.new
  result.kind = BNull
"""

when isMainModule:
    echo "Testing nimongo/bson.nim module..."
