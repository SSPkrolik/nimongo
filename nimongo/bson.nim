import oids
import sequtils
import tables
import streams
import sequtils
import strutils

# ------------- type: BsonKind -------------------#

type BsonKind* = enum
    BsonKindGeneric         = 0x00.char
    BsonKindDouble          = 0x01.char  ## 8-byte floating-point
    BsonKindStringUTF8      = 0x02.char  ## UTF-8 encoded C string
    BsonKindDocument        = 0x03.char
    BsonKindArray           = 0x04.char  ## Like document with numbers as keys
    BsonKindBinary          = 0x05.char
    BsonKindUndefined       = 0x06.char
    BsonKindOid             = 0x07.char  ## Mongo Object ID
    BsonKindBool            = 0x08.char
    BsonKindTimeUTC         = 0x09.char
    BsonKindNull            = 0x0A.char
    BsonKindRegexp          = 0x0B.char
    BsonKindDBPointer       = 0x0C.char
    BsonKindJSCode          = 0x0D.char
    BsonKindDeprecated      = 0x0E.char
    BsonKindJSCodeWithScope = 0x0F.char
    BsonKindInt32           = 0x10.char
    BsonKindTimestamp       = 0x11.char
    BsonKindInt64           = 0x12.char
    BsonKindMaximumKey      = 0x7F.char
    BsonKindMinimumKey      = 0xFF.char

converter toChar*(bk: BsonKind): char = bk.char  ## Convert BsonKind to char

# ------------- type: Bson -----------------------#

type
    Bson* = object of RootObj  ## Bson Node
        key: string
        case kind: BsonKind
        of BsonKindGeneric:    discard
        of BsonKindDouble:     valueFloat64:  float64    ## +
        of BsonKindStringUTF8: valueString:   string     ## +
        of BsonKindDocument:   valueDocument: seq[Bson]  ## +
        of BsonKindArray:      valueArray:    seq[Bson]
        of BsonKindBinary:     valueBinary:   cstring
        of BsonKindUndefined:  discard
        of BsonKindOid:        valueOid:      Oid
        of BsonKindBool:       valueBool:     bool
        of BsonKindNull:       discard
        of BsonKindInt64:      valueNull:     int64
        else: discard

converter toBson*(x: float64): Bson =
    ## Convert float64 to Bson object
    return Bson(key: "", kind: BsonKindDouble, valueFloat64: x)

converter toBson*(x: string): Bson =
    ## Convert string to Bson object
    return Bson(key: "", kind: BsonKindStringUTF8, valueString: x)

proc bson*(bs: Bson): string =
    ## Serialize Bson object into byte-stream
    echo bs
    case bs.kind
    of BsonKindDouble:
        let a = toSeq(cast[array[0..3, char]](bs.valueFloat64).items())
        return bs.kind & bs.key & char(0) & a.mapIt(string, $it).join() & char(0)
    of BsonKindStringUTF8:
        return bs.kind & bs.key & char(0) & bs.valueString & char(0)
    of BsonKindDocument:
        result = ""
        for val in bs.valueDocument:
            result = result & bson(val)
        result = bs.kind & bs.key & char(0) & result
        let
            size = len(result) + 4
            a = toSeq(cast[array[0..3, char]](size).items())
        result = a.mapIt(string, $it).join() & result
    else:
        raise new(Exception)

proc `$`*(bs: Bson): string =
    ## Serialize Bson document into readable string
    var ident = ""
    case bs.kind
    of BsonKindDouble:
        return "\"$#\": $#" % [bs.key, $bs.valueFloat64]
    of BsonKindStringUTF8:
        return "\"$#\": \"$#\"" % [bs.key, bs.valueString]
    of BsonKindDocument:
        var res: string = "{\n"
        ident &= "  "
        for i, item in bs.valueDocument:
            if i == len(bs.valueDocument) - 1: res = res & ident & $item & "\n"
            else: res = res & ident & $item & ",\n"
        ident = ident[0..len(ident) - 2]
        return res & "}"
    else:
        raise new(Exception)

# ------------- type: BsonDocument ---------------#

proc initBsonDocument*(): Bson =
    ## Create new top-level Bson document
    result = Bson(
        key: "",
        kind: BsonKindDocument,
        valueDocument: newSeq[Bson]()
    )

proc `()`*(bs: Bson, key: string, val: Bson): Bson =
    ## Add field to bson object
    result = bs
    var value: Bson = val
    value.key = key
    result.valueDocument.add(value)

when isMainModule:
    echo "Testing nimongo/bson.nim module..."
    var bdoc: Bson = initBsonDocument()("balance", 1000.23)("name", "John")
    echo bdoc
    for i in bdoc.bson():
        stdout.write(ord(i))
        stdout.write(" ")
    echo ""
