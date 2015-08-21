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

proc objToBytes*[T](x: T): string =
    ## Convert arbitrary data piece into series of bytes
    let a = toSeq(cast[array[0..3, char]](x).items())
    return a.mapIt(string, $it).join()

proc bytes*(bs: Bson): string =
    ## Serialize Bson object into byte-stream
    case bs.kind
    of BsonKindDouble:
        return bs.kind & bs.key & char(0) & objToBytes(bs.valueFloat64) & char(0)
    of BsonKindStringUTF8:
        return bs.kind & bs.key & char(0) & bs.valueString & char(0)
    of BsonKindDocument:
        result = ""
        for val in bs.valueDocument: result = result & bytes(val)
        result = bs.kind & bs.key & char(0) & result
        result = objToBytes(len(result) + 4) & result
    else:
        raise new(Exception)

proc `$`*(bs: Bson): string =
    ## Serialize Bson document into readable string
    var ident = ""
    proc stringify(bs: Bson): string =
        case bs.kind
        of BsonKindDouble:
            return "\"$#\": $#" % [bs.key, $bs.valueFloat64]
        of BsonKindStringUTF8:
            return "\"$#\": \"$#\"" % [bs.key, bs.valueString]
        of BsonKindDocument:
            var res: string = ""
            if bs.key != "":
                res = res & ident[0..len(ident) - 3] & "\"" & bs.key & "\":\n"
            res = res & ident & "{\n"
            ident = ident & "  "
            for i, item in bs.valueDocument:
                if i == len(bs.valueDocument) - 1: res = res & ident & stringify(item) & "\n"
                else: res = res & ident & stringify(item) & ",\n"
            ident = ident[0..len(ident) - 3]
            res = res & ident & "}"
            return res
        else:
            raise new(Exception)
    return stringify(bs)

proc initBsonDocument*(): Bson =
    ## Create new top-level Bson document
    result = Bson(
        key: "",
        kind: BsonKindDocument,
        valueDocument: newSeq[Bson]()
    )

proc `()`*(bs: Bson, key: string, val: Bson): Bson {.discardable.} =
    ## Add field to bson object
    result = bs
    var value: Bson = val
    value.key = key
    result.valueDocument.add(value)

when isMainModule:
    echo "Testing nimongo/bson.nim module..."
    var bdoc: Bson = initBsonDocument()(
        "balance", 1000.23)(
        "name", "John")(
        "surname", "Smith")(
        "subdoc", initBsonDocument()(
            "salary", 500.0
        )
    )
    echo bdoc
    for i in bdoc.bytes():
        stdout.write(ord(i))
        stdout.write(" ")
    echo ""
