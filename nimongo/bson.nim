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

# ------------- type: BsonKind -------------------#

type BsonKind* = char

const
    BsonKindUnknown*         = 0x00.BsonKind  ##
    BsonKindDouble*          = 0x01.BsonKind  ## 64-bit floating-point
    BsonKindStringUTF8*      = 0x02.BsonKind  ## UTF-8 encoded C string
    BsonKindDocument*        = 0x03.BsonKind  ## Embedded document
    BsonKindArray*           = 0x04.BsonKind  ## Embedded array of Bson values
    BsonKindBinary*          = 0x05.BsonKind  ## Generic binary data
    BsonKindUndefined*       = 0x06.BsonKind  ## Some undefined value (deprecated)
    BsonKindOid*             = 0x07.BsonKind  ## Mongo Object ID
    BsonKindBool*            = 0x08.BsonKind  ## boolean value
    BsonKindTimeUTC*         = 0x09.BsonKind  ## int64 milliseconds (Unix epoch time)
    BsonKindNull*            = 0x0A.BsonKind  ## nil value stored in Mongo
    BsonKindRegexp*          = 0x0B.BsonKind  ## Regular expression and options
    BsonKindDBPointer*       = 0x0C.BsonKind  ## Pointer to 'db.col._id'
    BsonKindJSCode*          = 0x0D.BsonKind  ## -
    BsonKindDeprecated*      = 0x0E.BsonKind  ## -
    BsonKindJSCodeWithScope* = 0x0F.BsonKind  ## -
    BsonKindInt32*           = 0x10.BsonKind  ## 32-bit integer number
    BsonKindTimestamp*       = 0x11.BsonKind  ## -
    BsonKindInt64*           = 0x12.BsonKind  ## 64-bit integer number
    BsonKindMaximumKey*      = 0x7F.BsonKind  ## Maximum MongoDB comparable value
    BsonKindMinimumKey*      = 0xFF.BsonKind  ## Minimum MongoDB comparable value

type BsonSubtype* = char

const
    BsonSubtypeGeneric*      = 0x00.BsonSubtype  ##
    BsonSubtypeFunction*     = 0x01.BsonSubtype  ##
    BsonSubtypeBinaryOld*    = 0x02.BsonSubtype  ##
    BsonSubtypeUuidOld*      = 0x03.BsonSubtype  ##
    BsonSubtypeUuid*         = 0x04.BsonSubtype  ##
    BsonSubtypeMd5*          = 0x05.BsonSubtype  ##
    BsonSubtypeUserDefined*  = 0x80.BsonSubtype  ##

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
    raise newException(Exception, "Wrong node kind: " & $ord(bs.kind))

proc toBson*(x: Oid): Bson =
    ## Convert Mongo Object Id to Bson object
    return Bson(kind: BsonKindOid, valueOid: x)

proc toOid*(x: Bson): Oid =
    ## Convert Bson to Mongo Object ID
    return x.valueOid

proc toBson*(x: float64): Bson =
    ## Convert float64 to Bson object
    return Bson(kind: BsonKindDouble, valueFloat64: x)

proc toFloat64*(x: Bson): float64 =
    ## Convert Bson object to float64
    return x.valueFloat64

proc toBson*(x: string): Bson =
    ## Convert string to Bson object
    return Bson(kind: BsonKindStringUTF8, valueString: x)

proc toString*(x: Bson): string =
    ## Convert Bson to UTF8 string
    case x.kind
    of BsonKindStringUTF8:
        return x.valueString
    else:
        raiseWrongNodeException(x)

proc toBson*(x: int64): Bson =
    ## Convert int64 to Bson object
    return Bson(kind: BsonKindInt64, valueInt64: x)

proc toInt64*(x: Bson): int64 =
    ## Convert Bson object to int
    case x.kind
    of BsonKindInt64:
        return int64(x.valueInt64)
    of BsonKindInt32:
        return int64(x.valueInt32)
    else:
        raiseWrongNodeException(x)

proc toBson*(x: int32): Bson =
    ## Convert int32 to Bson object
    return Bson(kind: BsonKindInt32, valueInt32: x)

proc toInt32*(x: Bson): int32 =
    ## Convert Bson to int32
    case x.kind
    of BsonKindInt64:
        return int32(x.valueInt64)
    of BsonKindInt32:
        return int32(x.valueInt32)
    else:
        raiseWrongNodeException(x)

proc toInt*(x: Bson): int =
    ## Convert Bson to int whether it is int32 or int64
    case x.kind
    of BsonKindInt64:
        return int(x.valueInt64)
    of BsonKindInt32:
        return int(x.valueInt32)
    else:
        raiseWrongNodeException(x)

proc toBson*(x: int): Bson =
    ## Convert int to Bson object
    return Bson(kind: BsonKindInt64, valueInt64: x)

proc toBson*(x: bool): Bson =
    ## Convert bool to Bson object
    return Bson(kind: BsonKindBool, valueBool: x)

proc toBool*(x: Bson): bool =
    ## Convert Bson object to bool
    return x.valueBool

proc toBson*(x: Time): Bson =
    ## Convert Time to Bson object
    return Bson(kind: BsonKindTimeUTC, valueTime: x)

proc toTime*(x: Bson): Time =
    ## Convert Bson object to Time
    return x.valueTime

proc toBson*(x: BsonTimestamp): Bson =
    ## Convert inner BsonTimestamp to Bson object
    return Bson(kind: BsonKindTimestamp, valueTimestamp: x)

proc toTimestamp*(x: Bson): BsonTimestamp =
    ## Convert Bson object to inner BsonTimestamp type
    return x.valueTimestamp

proc toBson*(x: MD5Digest): Bson =
    ## Convert MD5Digest to Bson object
    return Bson(kind: BsonKindBinary, subtype: BsonSubtypeMd5, valueDigest: x)

proc toBson*(x: var MD5Context): Bson =
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
        int64ToBytes(int64(bs.valueTime.toUnix() * 1000), res)
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
    result = Bson(kind: BsonKindDocument,
                  valueDocument: initOrderedTable[string, Bson]())

proc newBsonDocument*(): Bson =
    ## Create new empty Bson document
    result = Bson(kind: BsonKindDocument,
                  valueDocument: initOrderedTable[string, Bson]())

proc newBsonArray*(): Bson =
    ## Create new Bson array
    result = Bson(
        kind: BsonKindArray,
        valueArray: newSeq[Bson]()
    )

proc initBsonArray*(): Bson {.deprecated.} =
    ## Create new Bson array
    return newBsonArray()

template B*: untyped =
    newBsonDocument()

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

template toBson*(b: Bson): Bson = b
    ##

proc toBsonAUX*(keyVals: openArray[tuple[key: string, val: Bson]]): Bson =
    ## Generic constructor for BSON data.	
    result = newBsonDocument()
    for key, val in items(keyVals): result[key] = val

proc toBson(x: NimNode): NimNode {.compileTime.} =
  ## Convert NimNode into BSON document
  case x.kind

  of nnkBracket:
    result = newNimNode(nnkBracket)
    for i in 0 ..< x.len():
        result.add(toBson(x[i]))
    result = newCall("toBson", result)

  of nnkTableConstr:
    result = newNimNode(nnkTableConstr)
    for i in 0 ..< x.len():
        x[i].expectKind(nnkExprColonExpr)
        result.add(newNimNode(nnkExprColonExpr).add(x[i][0]).add(toBson(x[i][1])))
    result = newCall("toBsonAUX", result)

  of nnkCurly:
    result = newCall("newBsonDocument")
    x.expectLen(0)

  else:
    result = newCall("toBson", x)

macro `%*`*(x: untyped): Bson =
    ## Perform dict-like structure conversion into bson
    result = toBson(x)

template B*(key: string, val: Bson): Bson =  ## Shortcut for `newBsonDocument`
    let b = newBsonDocument()
    b[key] = val
    b

template B*[T](key: string, values: seq[T]): Bson =
    let b = newBsonDocument()
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
    if x.kind == BsonKindBinary:
        case x.subtype:
        of BsonSubtypeGeneric:     return x.valueGeneric
        of BsonSubtypeFunction:    return x.valueFunction
        of BsonSubtypeBinaryOld:   return x.valueBinOld
        of BsonSubtypeUuidOld:     return x.valueUuidOld
        of BsonSubtypeUuid:        return x.valueUuid
        of BsonSubtypeUserDefined: return x.valueUserDefined
        else:
            raiseWrongNodeException(x)
    else:
        raiseWrongNodeException(x)

proc binuser*(bindata: string): Bson =
    ## Create new binray Bson object with 'user-defined' subtype
    return Bson(
        kind: BsonKindBinary,
        subtype: BsonSubtypeUserDefined,
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

proc len*(bs: Bson):int =
    if bs.kind == BsonKindArray:
        result = bs.valueArray.len
    elif bs.kind == BsonKindDocument:
        result = bs.valueDocument.len
    else:
        raiseWrongNodeException(bs)


proc add*[T](bs: Bson, value: T): Bson {.discardable.} =
    result = bs
    result.valueArray.add(value.toBson())

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

proc readStr(s: Stream, length: int, result: var string) =
    result.setLen(length)
    if length != 0:
      var L = readData(s, addr(result[0]), length)
      if L != length: setLen(result, L)

proc newBsonDocument*(s: Stream): Bson =
    ## Create new Bson document from byte stream
    var buf = ""
    discard s.readInt32()   ## docSize
    result = newBsonDocument()
    var docStack = @[result]
    while docStack.len != 0:
        let kind: BsonKind = s.readChar()
        if kind == BsonKindUnknown:
            docStack.setLen(docStack.len - 1) # End of doc. pop stack.
            continue

        let doc = docStack[^1]

        discard s.readLine(buf)
        var sub: ptr Bson
        case doc.kind
        of BsonKindDocument:
            sub = addr doc.valueDocument.mgetOrPut(buf, nil)
        of BsonKindArray:
            doc.valueArray.add(nil)
            sub = addr doc.valueArray[^1]
        else:
            assert(false, "Internal error")

        case kind:
        of BsonKindDouble:
            sub[] = s.readFloat64().toBson()
        of BsonKindStringUTF8:
            s.readStr(s.readInt32() - 1, buf)
            discard s.readChar()
            sub[] = buf.toBson()
        of BsonKindDocument:
            discard s.readInt32()   ## docSize
            let subdoc = newBsonDocument()
            sub[] = subdoc
            docStack.add(subdoc)
        of BsonKindArray:
            discard s.readInt32()   ## docSize
            let subarr = newBsonArray()
            sub[] = subarr
            docStack.add(subarr)
        of BsonKindBinary:
            let
                ds: int32 = s.readInt32()
                st: BsonSubtype = s.readChar().BsonSubtype
            if ds > 0:
                s.readStr(ds, buf)
            else:
                buf = ""
            case st:
            of BsonSubtypeMd5:
                sub[] = cast[ptr MD5Digest](buf.cstring)[].toBson()
            of BsonSubtypeGeneric:
                sub[] = bin(buf)
            of BsonSubtypeUserDefined:
                sub[] = binuser(buf)
            of BsonSubtypeUuid:
                sub[] = bin(buf)
            else:
                raise newException(Exception, "Unexpected subtype: " & $(st.int))
        of BsonKindUndefined:
            sub[] = undefined()
        of BsonKindOid:
            s.readStr(12, buf)
            sub[] = cast[ptr Oid](buf.cstring)[].toBson()
        of BsonKindBool:
            sub[] = if s.readChar() == 0.char: false.toBson() else: true.toBson()
        of BsonKindTimeUTC:
            let timeUTC: Bson = Bson(kind: BsonKindTimeUTC, valueTime: fromUnix((s.readInt64().float64 / 1000).int64))
            sub[] = timeUTC
        of BsonKindNull:
            sub[] = null()
        of BsonKindRegexp:
            # sub[] = regex(s.readLine().string(), seqCharToString(sorted(s.readLine().string, system.cmp)))
            sub[] = regex(s.readLine().string(), s.readLine().string)
        of BsonKindDBPointer:
            let
              refcol: string = s.readStr(s.readInt32() - 1)
              refoid: Oid = cast[ptr Oid](s.readStr(12).cstring)[]
            discard s.readChar()
            sub[] = dbref(refcol, refoid)
        of BsonKindJSCode:
            s.readStr(s.readInt32() - 1, buf)
            discard s.readChar()
            sub[] = js(buf)
        of BsonKindInt32:
            sub[] = s.readInt32().toBson()
        of BsonKindTimestamp:
            sub[] = cast[BsonTimestamp](s.readInt64()).toBson()
        of BsonKindInt64:
            sub[] = s.readInt64().toBson()
        of BsonKindMinimumKey:
            sub[] = minkey()
        of BsonKindMaximumKey:
            sub[] = maxkey()
        else:
            raise newException(Exception, "Unexpected kind: " & $kind)

proc initBsonDocument*(stream: Stream): Bson {.deprecated.} =
    return newBsonDocument(stream)

proc initBsonDocument*(bytes: string): Bson {.deprecated.} =
    ## Create new Bson document from byte string
    newBsonDocument(newStringStream(bytes))

proc newBsonDocument*(bytes: string): Bson =
    ## Create new Bson document from byte string
    newBsonDocument(newStringStream(bytes))

## Serialization/deserialization ext

template dbKey*(name: string) {.pragma.}

proc to*(b: Bson, T: typedesc): T =
    when T is seq:
        result.setLen(b.len)
        var i = 0
        for c in b:
            result[i] = c.to(type(result[i]))
            inc i
    elif T is string:
        result = b.toString
    elif T is int|int8|int16|int32|uint|uint8|uint16|uint32:
        when b.toInt is T:
            result = b.toInt
        else:
            result = b.toInt.T
    elif T is int64|uint64:
        when b.toInt64 is T:
            result = b.toInt64
        else:
            result = b.toInt64.T
    elif T is float:
        result = b.toFloat64
    elif T is bool:
        result = b.toBool
    elif T is enum:
        result = parseEnum[T](b.toString)
    elif T is object|tuple|ref object:
        when T is ref object:
            if b.kind == BsonKindNull:
                result = nil
                return
            result.new()
        for k, val in fieldPairs(result):
            var key = k
            when val.hasCustomPragma(dbKey):
                static: echo "has pragma dbKey "
                key = val.getCustomPragmaVal(dbKey)
            if key notin b:
                raise newException(Exception, "Key " & key & " not found for " & $T)
            val = b[key].to(type(val))
    else:
        {.error: "Unknown type".}

proc toBson*[T](entry: T): Bson =
    when T is array | seq | set:
        result = newBsonArray()
        for v in entry:
            result.add(toBson(v))
    elif T is object | tuple | ref object:
        when T is ref object:
            if entry.isNil:
                result = null()
                return
        result = newBsonDocument()
        for k, v in fieldPairs(entry):
            when v.hasCustomPragma(dbKey):
                result[v.getCustomPragmaVal(dbKey)] = toBson(v)
            else:
                result[k] = toBson(v)
    elif T is enum:
        result = toBson($entry)
    elif T is int8|int16|uint8|uint16|uint32:
        result = toBson(entry.int32)
    elif T is uint64:
        result = toBson(entry.int64)
    elif T is Table|TableRef:
        result = newBsonDocument()
        for k, v in entry:
            result[k] = toBson(v)
    else:
        {.error: "toBson " & T & " can't serialize".}

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
    let recovered = newBsonDocument(bbytes)
    echo "RECOVERED: ", recovered

    var bdoc2 = newBsonArray()
    bdoc2 = bdoc2.add(2)
    bdoc2 = bdoc2.add(2)
    echo bdoc2
