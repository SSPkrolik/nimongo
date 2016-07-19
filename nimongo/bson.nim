import algorithm
import base64
import macros
import md5
import oids
import sequtils
import streams
import strutils
import times
import tables

import timeit

# ------------- type: BsonKind -------------------#

type BsonKind* = enum
    BsonKindUnknown         = 0x00.char  ##
    BsonKindDouble          = 0x01.char  ## 64-bit floating-point
    BsonKindStringUTF8      = 0x02.char  ## UTF-8 encoded C string
    BsonKindDocument        = 0x03.char  ## Embedded document
    BsonKindArray           = 0x04.char  ## Embedded array of Bson values
    BsonKindBinary          = 0x05.char  ## Generic binary data
    BsonKindUndefined       = 0x06.char  ## Some undefined value (deprecated)
    BsonKindOid             = 0x07.char  ## Mongo Object ID
    BsonKindBool            = 0x08.char  ## boolean value
    BsonKindTimeUTC         = 0x09.char  ## int64 milliseconds (Unix epoch time)
    BsonKindNull            = 0x0A.char  ## nil value stored in Mongo
    BsonKindRegexp          = 0x0B.char  ## Regular expression and options
    BsonKindDBPointer       = 0x0C.char  ## Pointer to 'db.col._id'
    BsonKindJSCode          = 0x0D.char  ## -
    BsonKindDeprecated      = 0x0E.char  ## -
    BsonKindJSCodeWithScope = 0x0F.char  ## -
    BsonKindInt32           = 0x10.char  ## 32-bit integer number
    BsonKindTimestamp       = 0x11.char  ## -
    BsonKindInt64           = 0x12.char  ## 64-bit integer number
    BsonKindMaximumKey      = 0x7F.char  ## Maximum MongoDB comparable value
    BsonKindMinimumKey      = 0xFF.char  ## Minimum MongoDB comparable value

type BsonSubtype* = enum
    BsonSubtypeGeneric      = 0x00.char  ##
    BsonSubtypeFunction     = 0x01.char  ##
    BsonSubtypeBinaryOld    = 0x02.char  ##
    BsonSubtypeUuidOld      = 0x03.char  ##
    BsonSubtypeUuid         = 0x04.char  ##
    BsonSubtypeMd5          = 0x05.char  ##
    BsonSubtypeUserDefined  = 0x80.char  ##

converter toChar*(bk: BsonKind): char =
    ## Convert BsonKind to char
    return bk.char

converter toChar*(sub: BsonSubtype): char =
    ## Convert BsonSubtype to char
    return sub.char

converter toBsonKind*(c: char): BsonKind =
    ## Convert char to BsonKind
    return c.BsonKind

# ------------- type: Bson -----------------------#

type
  BsonTimestamp* = object ## Internal MongoDB type used by mongos instances
    increment*: int32
    timestamp*: int32

  Bson* = ref object of RootObj  ## Bson Node
    case kind*: BsonKind
    of BsonKindDouble:           valueFloat64:     float64
    of BsonKindStringUTF8:       valueString:      string
    of BsonKindDocument:         valueDocument:    OrderedTable[string, Bson]
    of BsonKindArray:            valueArray:       seq[Bson]
    of BsonKindBinary:
      case subtype:                                BsonSubtype
      of BsonSubtypeGeneric:     valueGeneric:     string
      of BsonSubtypeFunction:    valueFunction:    string
      of BsonSubtypeBinaryOld:   valueBinOld:      string
      of BsonSubtypeUuidOld:     valueUuidOld:     string
      of BsonSubtypeUuid:        valueUuid:        string
      of BsonSubtypeMd5:         valueDigest:      MD5Digest
      of BsonSubtypeUserDefined: valueUserDefined: string
      else: discard
    of BsonKindUndefined:        discard
    of BsonKindOid:              valueOid:         Oid
    of BsonKindBool:             valueBool:        bool
    of BsonKindTimeUTC:          valueTime:        Time
    of BsonKindNull:             discard
    of BsonKindRegexp:
                                 regex:            string
                                 options:          string
    of BsonKindDBPointer:
                                 refCol:           string
                                 refOid:           Oid
    of BsonKindJSCode:           valueCode:        string
    of BsonKindDeprecated:       valueDepr:        string
    of BsonKindJSCodeWithScope:  valueCodeWS:      string
    of BsonKindInt32:            valueInt32:       int32
    of BsonKindTimestamp:        valueTimestamp:   BsonTimestamp
    of BsonKindInt64:            valueInt64:       int64
    of BsonKindMaximumKey:       discard
    of BsonKindMinimumKey:       discard
    else:                        discard

  GeoPoint = array[0..1, float64]   ## Represents Mongo Geo Point

proc raiseWrongNodeException(bs: Bson) =
    raise newException(Exception, "Wrong node kind: " & $bs.kind)

converter toBson*(x: Oid): Bson =
    ## Convert Mongo Object Id to Bson object
    return Bson(kind: BsonKindOid, valueOid: x)

converter toOid*(x: Bson): Oid =
    ## Convert Bson to Mongo Object ID
    return x.valueOid

converter toBson*(x: float64): Bson =
    ## Convert float64 to Bson object
    return Bson(kind: BsonKindDouble, valueFloat64: x)

converter toFloat64*(x: Bson): float64 =
    ## Convert Bson object to float64
    return x.valueFloat64

converter toBson*(x: string): Bson =
    ## Convert string to Bson object
    if x == nil:
        return Bson(kind: BsonKindNull)
    else:
        return Bson(kind: BsonKindStringUTF8, valueString: x)

converter toString*(x: Bson): string =
    ## Convert Bson to UTF8 string
    case x.kind
    of BsonKindStringUTF8:
        return x.valueString
    of BsonKindNull:
        return nil
    else:
        raiseWrongNodeException(x)

converter toBson*(x: int64): Bson =
    ## Convert int64 to Bson object
    return Bson(kind: BsonKindInt64, valueInt64: x)

converter toInt64*(x: Bson): int64 =
    ## Convert Bson object to int
    case x.kind
    of BsonKindInt64:
        return int64(x.valueInt64)
    of BsonKindInt32:
        return int64(x.valueInt32)
    else:
        raiseWrongNodeException(x)

converter toBson*(x: int32): Bson =
    ## Convert int32 to Bson object
    return Bson(kind: BsonKindInt32, valueInt32: x)

converter toInt32*(x: Bson): int32 =
    ## Convert Bson to int32
    case x.kind
    of BsonKindInt64:
        return int32(x.valueInt64)
    of BsonKindInt32:
        return int32(x.valueInt32)
    else:
        raiseWrongNodeException(x)

converter toInt*(x: Bson): int =
    ## Convert Bson to int whether it is int32 or int64
    case x.kind
    of BsonKindInt64:
        return int(x.valueInt64)
    of BsonKindInt32:
        return int(x.valueInt32)
    else:
        raiseWrongNodeException(x)

converter toBson*(x: int): Bson =
    ## Convert int to Bson object
    return Bson(kind: BsonKindInt64, valueInt64: x)

converter toBson*(x: bool): Bson =
    ## Convert bool to Bson object
    return Bson(kind: BsonKindBool, valueBool: x)

converter toBool*(x: Bson): bool =
    ## Convert Bson object to bool
    return x.valueBool

converter toBson*(x: Time): Bson =
    ## Convert Time to Bson object
    return Bson(kind: BsonKindTimeUTC, valueTime: x)

converter toTime*(x: Bson): Time =
    ## Convert Bson object to Time
    return x.valueTime

converter toBson*(x: BsonTimestamp): Bson =
    ## Convert inner BsonTimestamp to Bson object
    return Bson(kind: BsonKind.BsonKindTimestamp, valueTimestamp: x)

converter toTimestamp*(x: Bson): BsonTimestamp =
    ## Convert Bson object to inner BsonTimestamp type
    return x.valueTimestamp

converter toBson*(x: MD5Digest): Bson =
    ## Convert MD5Digest to Bson object
    return Bson(kind: BsonKindBinary, subtype: BsonSubtypeMd5, valueDigest: x)

converter toBson*(x: var MD5Context): Bson =
    ## Convert MD5Context to Bson object (still digest from current context).
    ## :WARNING: MD5Context is finalized during conversion.
    var digest: MD5Digest
    x.md5Final(digest)
    return Bson(kind: BsonKindBinary, subtype: BsonSubtypeMd5, valueDigest: digest)

proc podValueToBytesAtOffset[T](x: T, res: var string, off: int) {.inline.} =
    assert(off + sizeof(x) <= res.len)
    copyMem(addr res[off], unsafeAddr x, sizeof(x))

proc podValueToBytes[T](x: T, res: var string) {.inline.} =
    let off = res.len
    res.setLen(off + sizeof(x))
    podValueToBytesAtOffset(x, res, off)

proc int32ToBytesAtOffset*(x: int32, res: var string, off: int) =
    podValueToBytesAtOffset(x, res, off)

proc int32ToBytes*(x: int32, res: var string) {.inline.} =
    ## Convert int32 data piece into series of bytes
    podValueToBytes(x, res)

proc float64ToBytes*(x: float64, res: var string) {.inline.} =
    ## Convert float64 data piece into series of bytes
    podValueToBytes(x, res)

proc int64ToBytes*(x: int64, res: var string) {.inline.} =
    ## Convert int64 data piece into series of bytes
    podValueToBytes(x, res)

proc boolToBytes*(x: bool, res: var string) {.inline.} =
    ## Convert bool data piece into series of bytes
    podValueToBytes(if x: 1'u8 else: 0'u8, res)

proc oidToBytes*(x: Oid, res: var string) {.inline.} =
    ## Convert Mongo Object ID data piece into series to bytes
    podValueToBytes(x, res)

proc toBytes*(bs: Bson, res: var string) =
    ## Serialize Bson object into byte-stream
    case bs.kind
    of BsonKindDouble:
        float64ToBytes(bs.valueFloat64, res)
    of BsonKindStringUTF8:
        int32ToBytes(int32(bs.valueString.len + 1), res)
        res &= bs.valueString
        res &= char(0)
    of BsonKindDocument:
        let off = res.len
        res.setLen(off + sizeof(int32)) # We shall write the length in here...
        for key, val in bs.valueDocument:
            res &= val.kind
            res &= key
            res &= char(0)
            val.toBytes(res)
        res &= char(0)
        int32ToBytesAtOffset(int32(res.len - off), res, off)
    of BsonKindArray:
        let off = res.len
        res.setLen(off + sizeof(int32)) # We shall write the length in here...
        for i, val in bs.valueArray:
            res &= val.kind
            res &= $i
            res &= char(0)
            val.toBytes(res)
        res &= char(0)
        int32ToBytesAtOffset(int32(res.len - off), res, off)
    of BsonKindBinary:
        case bs.subtype
        of BsonSubtypeMd5:
            var sdig: string = newStringOfCap(16)
            for i in 0..<bs.valueDigest.len():
                add(sdig, bs.valueDigest[i].char)
            int32ToBytes(16, res)
            res &= bs.subtype.char & sdig
        of BsonSubtypeGeneric:
            int32ToBytes(int32(bs.valueGeneric.len), res)
            res &= bs.subtype.char & bs.valueGeneric
        of BsonSubtypeUserDefined:
            int32ToBytes(int32(bs.valueUserDefined.len), res)
            res &= bs.subtype.char & bs.valueUserDefined
        else:
            raiseWrongNodeException(bs)
    of BsonKindUndefined:
        discard
    of BsonKindOid:
        oidToBytes(bs.valueOid, res)
    of BsonKindBool:
        boolToBytes(bs.valueBool, res)
    of BsonKindTimeUTC:
        int64ToBytes(int64(bs.valueTime.toSeconds() * 1000), res)
    of BsonKindNull:
        discard
    of BsonKindRegexp:
        res &= bs.regex & char(0) & bs.options & char(0)
    of BsonKindDBPointer:
        int32ToBytes(int32(bs.refCol.len + 1), res)
        res &= bs.refCol & char(0)
        oidToBytes(bs.refOid, res)
    of BsonKindJSCode:
        int32ToBytes(int32(bs.valueCode.len + 1), res)
        res &= bs.valueCode & char(0)
    of BsonKindInt32:
        int32ToBytes(bs.valueInt32, res)
    of BsonKindTimestamp:
        int64ToBytes(cast[ptr int64](addr bs.valueTimestamp)[], res)
    of BsonKindInt64:
        int64ToBytes(bs.valueInt64, res)
    of BsonKindMinimumKey, BsonKindMaximumKey:
        discard
    else:
        raiseWrongNodeException(bs)

proc `$`*(bs: Bson): string

proc bytes*(bs: Bson): string =
    result = ""
    bs.toBytes(result)

proc `$`*(bs: Bson): string =
    ## Serialize Bson document into readable string
    proc stringify(bs: Bson, indent: string): string =
        if bs.isNil: return "null"
        case bs.kind
        of BsonKindDouble:
            return $bs.valueFloat64
        of BsonKindStringUTF8:
            return "\"" & bs.valueString & "\""
        of BsonKindDocument:
            var res = "{\n"
            let ln = bs.valueDocument.len
            var i = 0
            let newIndent = indent & "    "
            for k, v in bs.valueDocument:
                res &= newIndent
                res &= "\"" & k & "\" : "
                res &= stringify(v, newIndent)
                if i != ln - 1:
                    res &= ","
                inc i
                res &= "\n"
            res &= indent & "}"
            return res
        of BsonKindArray:
            var res = "[\n"
            let newIndent = indent & "    "
            for i, v in bs.valueArray:
                res &= newIndent
                res &= stringify(v, newIndent)
                if i != bs.valueArray.len - 1:
                    res &= ","
                res &= "\n"
            res &= indent & "]"
            return res
        of BsonKindBinary:
            case bs.subtype
            of BsonSubtypeMd5:
                return "{\"$$md5\": \"$#\"}" % [$bs.valueDigest]
            of BsonSubtypeGeneric:
                return "{\"$$bindata\": \"$#\"}" % [base64.encode(bs.valueGeneric)]
            of BsonSubtypeUserDefined:
                return "{\"$$bindata\": \"$#\"}" % [base64.encode(bs.valueUserDefined)]
            else:
                raiseWrongNodeException(bs)
        of BsonKindUndefined:
            return "undefined"
        of BsonKindOid:
            return "{\"$$oid\": \"$#\"}" % [$bs.valueOid]
        of BsonKindBool:
            return if bs.valueBool == true: "true" else: "false"
        of BsonKindTimeUTC:
            return $bs.valueTime
        of BsonKindNull:
            return "null"
        of BsonKindRegexp:
            return "{\"$$regex\": \"$#\", \"$$options\": \"$#\"}" % [bs.regex, bs.options]
        of BsonKindDBPointer:
            let
              refcol = bs.refCol.split(".")[1]
              refdb  = bs.refCol.split(".")[0]
            return "{\"$$ref\": \"$#\", \"$$id\": \"$#\", \"$$db\": \"$#\"}" % [refcol, $bs.refOid, refdb]
        of BsonKindJSCode:
            return bs.valueCode ## TODO: make valid JSON here
        of BsonKindInt32:
            return $bs.valueInt32
        of BsonKindTimestamp:
            return "{\"$$timestamp\": $#}" % [$(cast[ptr int64](addr bs.valueTimestamp)[])]
        of BSonKindInt64:
            return $bs.valueInt64
        of BsonKindMinimumKey:
            return "{\"$$minkey\": 1}"
        of BsonKindMaximumKey:
            return "{\"$$maxkey\": 1}"
        else:
            raiseWrongNodeException(bs)
    return stringify(bs, "")

proc initBsonDocument*(): Bson {.deprecated.}=
    ## Create new top-level Bson document
    result.new
    result.kind = BsonKindDocument
    result.valueDocument = initOrderedTable[string, Bson]()

proc newBsonDocument*(): Bson =
    ## Create new empty Bson document
    result.new
    result.kind = BsonKindDocument
    result.valueDocument = initOrderedTable[string, Bson]()

proc initBsonArray*(): Bson =
    ## Create new Bson array
    result = Bson(
        kind: BsonKindArray,
        valueArray: newSeq[Bson]()
    )

template B*: expr =
    initBsonDocument()

proc `[]`*(bs: Bson, key: string): Bson =
    ## Get Bson document field
    if bs.kind == BsonKindDocument:
        return bs.valueDocument.getOrDefault(key)
    else:
        raiseWrongNodeException(bs)

proc `[]=`*(bs: Bson, key: string, value: Bson) =
  ## Modify Bson document field
  if bs.kind == BsonKindDocument:
      bs.valueDocument[key] = value
  else:
      raiseWrongNodeException(bs)

proc `[]`*(bs: Bson, key: int): Bson =
    ## Get Bson array item by index
    if bs.kind == BsonKindArray:
        return bs.valueArray[key]
    else:
        raiseWrongNodeException(bs)

proc `[]=`*(bs: Bson, key: int, value: Bson) =
    ## Modify Bson array element
    if bs.kind == BsonKindArray:
        bs.valueArray[key] = value

proc toBson*(keyVals: openArray[tuple[key: string, val: Bson]]): Bson =
    ## Generic constructor for BSON data.
    result = initBsonDocument()
    for key, val in items(keyVals): result[key] = val

proc toBson*[T](vals: openArray[T]): Bson =
    result = initBsonArray()
    for val in vals: result.add(toBson(val))

template toBson*(b: Bson): Bson = b
    ##

proc toBson(x: NimNode): NimNode {.compileTime.} =
  ## Convert NimNode into BSON document
  case x.kind

  of nnkBracket:
    result = newNimNode(nnkBracket)
    for i in 0 .. <x.len():
        result.add(toBson(x[i]))
    result = newCall("toBson", result)

  of nnkTableConstr:
    result = newNimNode(nnkTableConstr)
    for i in 0 .. <x.len():
        x[i].expectKind(nnkExprColonExpr)
        result.add(newNimNode(nnkExprColonExpr).add(x[i][0]).add(toBson(x[i][1])))
    result = newCall("toBson", result)

  of nnkCurly:
    result = newCall("initBsonDocument")
    x.expectLen(0)

  else:
    result = newCall("toBson", x)

macro `%*`*(x: expr): Bson =
    ## Perform dict-like structure conversion into bson
    result = toBson(x)

template B*(key: string, val: Bson): Bson =  ## Shortcut for _initBsonDocument
    let b = initBsonDocument()
    b[key] = val
    b

template B*[T](key: string, values: seq[T]): Bson =
    let b = initBsonDocument()
    b[key] = values
    b

proc dbref*(refcol: string, refoid: Oid): Bson =
    ## Create new DBRef (database reference) MongoDB bson type
    return Bson(kind: BsonKindDBPointer, refcol: refcol, refoid: refoid)

proc undefined*(): Bson =
    ## Create new Bson 'undefined' value
    return Bson(kind: BsonKindUndefined)

proc null*(): Bson =
    ## Create new Bson 'null' value
    return Bson(kind: BsonKindNull)

proc minkey*(): Bson =
    ## Create new Bson value representing 'Min key' bson type
    return Bson(kind: BsonKindMinimumKey)

proc maxkey*(): Bson =
    ## Create new Bson value representing 'Max key' bson type
    return Bson(kind: BsonKindMaximumKey)

proc regex*(pattern: string, options: string): Bson =
    ## Create new Bson value representing Regexp bson type
    return Bson(kind: BsonKindRegexp, regex: pattern, options: options)

proc js*(code: string): Bson =
    ## Create new Bson value representing JavaScript code bson type
    return Bson(kind: BsonKindJSCode, valueCode: code)

proc bin*(bindata: string): Bson =
    ## Create new binary Bson object with 'generic' subtype
    return Bson(
        kind: BsonKindBinary,
        subtype: BsonSubtypeGeneric,
        valueGeneric: bindata
    )

proc binstr*(x: Bson): string =
    return x.valueGeneric

proc binuser*(bindata: string): Bson =
    ## Create new binray Bson object with 'user-defined' subtype
    return Bson(
        kind: BsonKindBinary,
        subtype: BsonSubtype.BsonSubtypeUserDefined,
        valueUserDefined: bindata
    )

proc geo*(loc: GeoPoint): Bson =
    ## Convert array of two floats into Bson as MongoDB Geo-Point.
    return Bson(
        kind: BsonKindArray,
        valueArray: @[loc[0].toBson(), loc[1].toBson()]
    )

proc timeUTC*(time: Time): Bson =
  ## Create UTC datetime Bson object.
  return Bson(
    kind: BsonKindTimeUTC,
    valueTime: time
  )

proc `()`*(bs: Bson, key: string, val: Bson): Bson {.discardable, deprecated.} =
  ## Add field to bson object
  result = bs
  if bs.kind == BsonKindDocument:
      if not isNil(val):
          result.valueDocument[key] = val
      else:
          result.valueDocument[key] = null()
  else:
      raiseWrongNodeException(bs)

proc `()`*[T](bs: Bson, key: string, values: seq[T]): Bson {.discardable, deprecated.} =
    ## Add array field to bson object
    result = bs

    var arr: Bson = initBsonArray()

    for val in values:
        arr.valueArray.add(val)

    result.valueDocument[key] = arr

proc add*[T](bs: Bson, value: T): Bson {.discardable.} =
    result = bs
    result.valueArray.add(value)

proc del*(bs: Bson, key: string) =
    if bs.kind == BsonKindDocument:
        bs.valueDocument.del(key)
    else:
        raiseWrongNodeException(bs)

proc delete*(bs: Bson, idx: int) =
    if bs.kind == BsonKindArray:
        bs.valueArray.delete(idx)
    else:
        raiseWrongNodeException(bs)

proc del*(bs: Bson, idx: int) =
    if bs.kind == BsonKindArray:
        bs.valueArray.del(idx)
    else:
        raiseWrongNodeException(bs)

proc `{}`*(bs: Bson, keys: varargs[string]): Bson =
  var b = bs
  for k in keys:
    if b.kind == BsonKindDocument:
      b = b.valueDocument.getOrDefault(k)
      if b.isNil: return nil
    else:
      return nil
  return b

proc `{}=`*(bs: Bson, keys: varargs[string], value: Bson) =
  var bs = bs
  for i in 0..(keys.len-2):
    if isNil(bs[keys[i]]):
      bs[keys[i]] = newBsonDocument()
    bs = bs[keys[i]]
  bs[keys[^1]] = value

iterator items*(bs: Bson): Bson =
    ## Iterate over Bson document or array fields
    if bs.kind == BsonKindDocument:
        for _, v in bs.valueDocument:
            yield v
    elif bs.kind == BsonKindArray:
        for item in bs.valueArray:
            yield item

iterator pairs*(bs: Bson): tuple[key: string, val: Bson] =
    ## Iterate over Bson document
    if bs.kind == BsonKindDocument:
        for k, v in bs.valueDocument:
            yield (k, v)

proc contains*(bs: Bson, key: string): bool =
  ## Checks if Bson document has a specified field
  if bs.kind == BsonKindDocument:
    return key in bs.valueDocument
  else:
    return false

converter seqCharToString(x: openarray[char]): string =
    ## Converts sequence of chars to string
    result = newStringOfCap(len(x))
    for c in x: result = result & c

proc initBsonDocument*(stream: Stream): Bson =
    ## Create new Bson document from byte stream
    discard stream.readInt32()   ## docSize

    proc parseBson(s: Stream, doc: Bson): Bson =
        let kind: BsonKind = s.readChar()
        var name: TaintedString = ""
        discard s.readLine(name)
        case kind:
        of BsonKindDouble:
            doc[name.string] = s.readFloat64()
            return doc
        of BsonKindStringUTF8:
            let valueString: string = s.readStr(s.readInt32() - 1)
            discard s.readChar()
            doc[name.string] = valueString
            return doc
        of BsonKindDocument:
            let ds: int32 = stream.peekInt32()
            var subdoc = initBsonDocument(s)
            doc[name] = subdoc
            return doc
        of BsonKindArray:
            let ds: int32 = stream.peekInt32()
            var subdoc = initBsonDocument(s)
            var subarr = initBsonArray()
            subarr.valueArray = @[]
            for k, v in subdoc.valueDocument: subarr.valueArray.add(v)
            doc[name.string] = subarr
            return doc
        of BsonKindBinary:
            let
                ds: int32 = s.readInt32()
                st: BsonSubtype = s.readChar().BsonSubtype
            case st:
            of BsonSubtypeMd5:
                doc[name.string] = cast[MD5Digest](s.readStr(ds).cstring)
                return doc
            of BsonSubtypeGeneric:
                doc[name.string] = bin(s.readStr(ds))
                return doc
            of BsonSubtype.BsonSubtypeUserDefined:
                doc[name.string] = binuser(s.readStr(ds))
                return doc
            else:
                raise newException(Exception, "Unexpected subtype: " & $st)
        of BsonKindUndefined:
            doc[name.string] = undefined()
            return doc
        of BsonKindOid:
            let valueOid: Oid = cast[Oid](s.readStr(12).cstring)
            doc[name.string] = valueOid
            return doc
        of BsonKindBool:
            doc[name.string] = if s.readChar() == 0.char: false else: true
            return doc
        of BsonKindTimeUTC:
            let timeUTC: Bson = Bson(kind: BsonKindTimeUTC, valueTime: fromSeconds(s.readInt64().float64 / 1000))
            doc[name.string] = timeUTC
            return doc
        of BsonKindNull:
            doc[name.string] = null()
            return doc
        of BsonKindRegexp:
            doc[name.string] = regex(s.readLine().string(), seqCharToString(sorted(s.readLine().string, system.cmp)))
            return doc
        of BsonKindDBPointer:
            let
              refcol: string = s.readStr(s.readInt32() - 1)
              refoid: Oid = cast[Oid](s.readStr(12).cstring)
            discard s.readChar()
            doc[name.string] = dbref(refcol, refoid)
            return doc
        of BsonKindJSCode:
            let
              code: string = s.readStr(s.readInt32() - 1)
            discard s.readChar()
            doc[name.string] = js(code)
            return doc
        of BsonKindInt32:
            doc[name.string] = s.readInt32()
            return doc
        of BsonKindTimestamp:
            doc[name.string] = cast[BsonTimestamp](s.readInt64())
            return doc
        of BsonKindInt64:
            doc[name.string] = s.readInt64()
            return doc
        of BsonKindMinimumKey:
            doc[name.string] = minkey()
            return doc
        of BsonKindMaximumKey:
            doc[name.string] = maxkey()
            return doc
        else:
            raise newException(Exception, "Unexpected kind: " & $kind)

    var document: Bson = initBsonDocument()

    while stream.peekChar() != 0.char:
        document = parseBson(stream, document)
    discard stream.readChar()

    return document

proc initBsonDocument*(bytes: string): Bson =
    ## Create new Bson document from byte string
    initBsonDocument(newStringStream(bytes))

proc merge*(a, b: Bson): Bson =

    proc m_rec(a,b,r: Bson)=
        for k, v in a:
            if not b[k].isNil:
                r[k] = v.merge(b[k])
            else:
                r[k] = v

        for k, v in b:
            if a[k].isNil:
                r[k] = v

    if (a.kind == BsonKindDocument or a.kind == BsonKindArray) and
        (b.kind == BsonKindDocument or b.kind == BsonKindArray):
        result = newBsonDocument()
        m_rec(a,b,result)
    else:
        result = a

proc update*(a, b: Bson)=
    if (a.kind == BsonKindDocument or a.kind == BsonKindArray) and
        (b.kind == BsonKindDocument or b.kind == BsonKindArray):

        for k, v in a:
            if not b[k].isNil:
                a[k] = v.merge(b[k])

        for k, v in b:
            if a[k].isNil:
                a[k] = v

when isMainModule:
    echo "Testing nimongo/bson.nim module..."
    let oid = genOid()
    let bdoc: Bson = %*{
        "image": bin("12312l3jkalksjslkvdsdas"),
        "balance":       1000.23,
        "name":          "John",
        "someId":        oid,
        "someTrue":      true,
        "surname":       "Smith",
        "someNull":      null(),
        "minkey":        minkey(),
        "maxkey":        maxkey(),
        "digest":        "".toMd5(),
        "regexp-field":  regex("pattern", "ismx"),
        "undefined":     undefined(),
        "someJS":        js("function identity(x) {return x;}"),
        "someRef":       dbref("db.col", genOid()),
        "userDefined":   binuser("some-binary-data"),
        "someTimestamp": BsonTimestamp(increment: 1, timestamp: 1),
        "utcTime":       timeUTC(getTime()),
        "subdoc": %*{
            "salary": 500
        },
        "array": [
            %*{"string": "hello"},
            %*{"string" : "world"}
        ]
    }

    echo bdoc
    let bbytes = bdoc.bytes()
    let recovered = initBsonDocument(bbytes)
    echo "RECOVERED: ", recovered

    var bdoc2 = initBsonArray()
    bdoc2 = bdoc2.add(2)
    bdoc2 = bdoc2.add(2)
    echo bdoc2
