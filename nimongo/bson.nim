import algorithm
import base64
import macros
import marshal
import md5
import oids
import sequtils
import streams
import strutils
import times

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
    key: string
    case kind*: BsonKind
    of BsonKindDouble:           valueFloat64:     float64
    of BsonKindStringUTF8:       valueString:      string
    of BsonKindDocument:         valueDocument:    seq[Bson]
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

converter toBson*(x: Oid): Bson =
    ## Convert Mongo Object Id to Bson object
    return Bson(key: "", kind: BsonKindOid, valueOid: x)

converter toOid*(x: Bson): Oid =
    ## Convert Bson to Mongo Object ID
    return x.valueOid

converter toBson*(x: float64): Bson =
    ## Convert float64 to Bson object
    return Bson(key: "", kind: BsonKindDouble, valueFloat64: x)

converter toFloat64*(x: Bson): float64 =
    ## Convert Bson object to float64
    return x.valueFloat64

converter toBson*(x: string): Bson =
    ## Convert string to Bson object
    return Bson(key: "", kind: BsonKindStringUTF8, valueString: x)

converter toString*(x: Bson): string =
    ## Convert Bson to UTF8 string
    return x.valueString

converter toBson*(x: int64): Bson =
    ## Convert int64 to Bson object
    return Bson(key: "", kind: BsonKindInt64, valueInt64: x)

converter toInt64*(x: Bson): int64 =
    ## Convert Bson object to int
    return x.valueInt64

converter toBson*(x: int32): Bson =
    ## Convert int32 to Bson object
    return Bson(key: "", kind: BsonKindInt32, valueInt32: x)

converter toInt32*(x: Bson): int32 =
    ## Convert Bson to int32
    return x.valueInt32

converter toBson*(x: int): Bson =
    ## Convert int to Bson object
    return Bson(key: "", kind: BsonKindInt64, valueInt64: x)

converter toBson*(x: bool): Bson =
    ## Convert bool to Bson object
    return Bson(key: "", kind: BsonKindBool, valueBool: x)

converter toBool*(x: Bson): bool =
    ## Convert Bson object to bool
    return x.valueBool

converter toBson*(x: Time): Bson =
    ## Convert Time to Bson object
    return Bson(key: "", kind: BsonKindTimeUTC, valueTime: x)

converter toTime*(x: Bson): Time =
    ## Convert Bson object to Time
    return x.valueTime

converter toBson*(x: BsonTimestamp): Bson =
  ## Convert inner BsonTimestamp to Bson object
  return Bson(key: "", kind: BsonKind.BsonKindTimestamp, valueTimestamp: x)

converter toTimestamp*(x: Bson): BsonTimestamp =
  ## Convert Bson object to inner BsonTimestamp type
  return x.valueTimestamp

converter toBson*(x: MD5Digest): Bson =
  ## Convert MD5Digest to Bson object
  return Bson(key: "", kind: BsonKindBinary, subtype: BsonSubtypeMd5, valueDigest: x)

converter toBson*(x: var MD5Context): Bson =
  ## Convert MD5Context to Bson object (still digest from current context).
  ## :WARNING: MD5Context is finalized during conversion.
  var digest: MD5Digest
  x.md5Final(digest)
  return Bson(key: "", kind: BsonKindBinary, subtype: BsonSubtypeMd5, valueDigest: digest)

proc int32ToBytes*(x: int32): string =
    ## Convert int32 data piece into series of bytes
    result = newString(4).TaintedString
    copyMem(addr(result[0]), unsafeAddr x, 4)

proc float64ToBytes*(x: float64): string =
  ## Convert float64 data piece into series of bytes
  result = newString(8).TaintedString
  copyMem(addr(result[0]), unsafeAddr x, 8)

proc int64ToBytes*(x: int64): string =
  ## Convert int64 data piece into series of bytes
  result = newString(8).TaintedString
  copyMem(addr(result[0]), unsafeAddr x, 8)

proc boolToBytes*(x: bool): string =
  ## Convert bool data piece into series of bytes
  if x == true: return $char(1)
  else: return $char(0)

proc oidToBytes*(x: Oid): string =
  ## Convert Mongo Object ID data piece into series to bytes
  result = newString(12).TaintedString
  copyMem(addr(result[0]), unsafeAddr x, 12)

proc bytes*(bs: Bson): string =
    ## Serialize Bson object into byte-stream
    case bs.kind
    of BsonKindDouble:
        return bs.kind & bs.key & char(0) & float64ToBytes(bs.valueFloat64)
    of BsonKindStringUTF8:
        return bs.kind & bs.key & char(0) & int32ToBytes(len(bs.valueString).int32 + 1) & bs.valueString & char(0)
    of BsonKindDocument:
        result = ""
        for val in bs.valueDocument: result = result & bytes(val)
        if bs.key != "":
            result = result & char(0)
            result = bs.kind & bs.key & char(0) & int32ToBytes(int32(len(result) + 4)) & result
        else:
            result = result & char(0)
            result = int32ToBytes(int32(len(result) + 4)) & result
    of BsonKindArray:
        result = ""
        for val in bs.valueArray: result = result & bytes(val)
        if bs.key != "":
            result = result & char(0)
            result = bs.kind & bs.key & char(0) & int32ToBytes(int32(len(result) + 4)) & result
        else:
            result = result & char(0)
            result = int32ToBytes(int32(len(result) + 4)) & result
    of BsonKindBinary:
        case bs.subtype
        of BsonSubtypeMd5:
            var sdig: string = newStringOfCap(16)
            for i in 0..<bs.valueDigest.len():
                add(sdig, bs.valueDigest[i].char)
            return bs.kind & bs.key & char(0) & int32ToBytes(int32(16)) & bs.subtype.char & sdig
        of BsonSubtypeGeneric:
            return bs.kind & bs.key & char(0) & int32ToBytes(int32(bs.valueGeneric.len())) & bs.subtype.char & bs.valueGeneric
        of BsonSubtypeUserDefined:
            return bs.kind & bs.key & char(0) & int32ToBytes(int32(bs.valueUserDefined.len())) & bs.subtype.char & bs.valueUserDefined
        else:
            raise new(Exception)
    of BsonKindUndefined:
        return bs.kind & bs.key & char(0)
    of BsonKindOid:
        return bs.kind & bs.key & char(0) & oidToBytes(bs.valueOid)
    of BsonKindBool:
        return bs.kind & bs.key & char(0) & boolToBytes(bs.valueBool)
    of BsonKindTimeUTC:
        return bs.kind & bs.key & char(0) & int64ToBytes(int64(bs.valueTime.toSeconds() * 1000))
    of BsonKindNull:
        return bs.kind & bs.key & char(0)
    of BsonKindRegexp:
        return bs.kind & bs.key & char(0) & bs.regex & char(0) & bs.options & char(0)
    of BsonKindDBPointer:
        return bs.kind & bs.key & char(0) & int32ToBytes(int32(len(bs.refCol)) + 1) & bs.refCol & char(0) & oidToBytes(bs.refOid)
    of BsonKindJSCode:
        return bs.kind & bs.key & char(0) & int32ToBytes(int32(len(bs.valueCode)) + 1) & bs.valueCode & char(0)
    of BsonKindInt32:
        return bs.kind & bs.key & char(0) & int32ToBytes(bs.valueInt32)
    of BsonKindTimestamp:
        return bs.kind & bs.key & char(0) & int64ToBytes(cast[ptr int64](addr bs.valueTimestamp)[])
    of BsonKindInt64:
        return bs.kind & bs.key & char(0) & int64ToBytes(bs.valueInt64)
    of BsonKindMinimumKey:
        return bs.kind & bs.key & char(0)
    of BsonKindMaximumKey:
        return bs.kind & bs.key & char(0)
    else:
        echo "BYTES: ", bs.kind
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
        of BsonKindArray:
            var res: string = ""
            res = res & "\"" & bs.key & "\": {"
            ident = ident & "  "
            for i, item in bs.valueArray:
                if i == len(bs.valueArray) - 1: res = res & stringify(item)
                else: res = res & stringify(item) & ", "
            ident = ident[0..len(ident) - 3]
            res = res & "}"
            return res
        of BsonKindBinary:
            case bs.subtype
            of BsonSubtypeMd5:
                return "\"$#\": {\"$$md5\": \"$#\"}" % [bs.key, $bs.valueDigest]
            of BsonSubtypeGeneric:
                return "\"$#\": {\"$$bindata\": \"$#\"}" % [bs.key, base64.encode(bs.valueGeneric)]
            of BsonSubtypeUserDefined:
                return "\"$#\": {\"$$bindata\": \"$#\"}" % [bs.key, base64.encode(bs.valueUserDefined)]
            else:
                raise new(Exception)
        of BsonKindUndefined:
            return "\"$#\": null" % [bs.key]
        of BsonKindOid:
            return "\"$#\": {\"$$oid\": \"$#\"}" % [bs.key, $bs.valueOid]
        of BsonKindBool:
            return "\"$#\": $#" % [bs.key, if bs.valueBool == true: "true" else: "false"]
        of BsonKindTimeUTC:
            return "\"$#\": $#" % [bs.key, $bs.valueTime]
        of BsonKindNull:
            return "\"$#\": null" % [bs.key]
        of BsonKindRegexp:
            return "\"$#\": {\"$$regex\": \"$#\", \"$$options\": \"$#\"}" % [bs.key, bs.regex, bs.options]
        of BsonKindDBPointer:
            let
              refcol = bs.refCol.split(".")[1]
              refdb  = bs.refCol.split(".")[0]
            return "\"$#\": {\"$$ref\": \"$#\", \"$$id\": \"$#\", \"$$db\": \"$#\"}" % [bs.key, refcol, $bs.refOid, refdb]
        of BsonKindJSCode:
            return "\"$#\": $#" % [bs.key, bs.valueCode] ## TODO: make valid JSON here
        of BsonKindInt32:
            return "\"$#\": $#" % [bs.key, $bs.valueInt32]
        of BsonKindTimestamp:
            return "\"$#\": {\"$$timestamp\": $#}" % [bs.key, $(cast[ptr int64](addr bs.valueTimestamp)[])]
        of BSonKindInt64:
            return "\"$#\": $#" % [bs.key, $bs.valueInt64]
        of BsonKindMinimumKey:
            return "\"$#\": {\"$$minkey\": 1}" % [bs.key]
        of BsonKindMaximumKey:
            return "\"$#\": {\"$$maxkey\": 1}" % [bs.key]
        else:
            echo bs.kind
            raise new(Exception)
    return stringify(bs)

proc initBsonDocument*(): Bson =
  ## Create new top-level Bson document
  result.new
  result.key = ""
  result.kind = BsonKindDocument
  result.valueDocument = @[]

proc newBsonDocument*(): Bson =
  ## Create new empty Bson document
  result.new
  result.key = ""
  result.kind = BsonKindDocument
  result.valueDocument = @[]

proc initBsonArray*(): Bson =
    ## Create new Bson array
    result = Bson(
        key: "",
        kind: BsonKindArray,
        valueArray: newSeq[Bson]()
    )

template B*: expr =
    initBsonDocument()

proc toBson(x: NimNode, child: NimNode = nil): NimNode =
  ## Convert NimNode into BSON document
  case x.kind
  of nnkCurly:
    if x.len() == 0:
      var call = newNimNode(nnkCall)
      call.add(ident("initBsonDocument"))
      return call
  of nnkTableConstr:
    var call = newNimNode(nnkCall)
    call.add(ident("initBsonDocument"))
    for i in 0 .. <x.len():
      if x[i].kind == nnkExprColonExpr:
        var parentCall = newNimNode(nnkCall)
        parentCall.add(call)
        call = toBson(x[i], parentCall)
    return call
  of nnkExprColonExpr:
    child.add(x[0])
    if x[1].kind == nnkBracket:
      child.add(prefix(x[1], "@"))
    elif x[1].kind == nnkTableConstr:
      child.add(toBson(x[1]))
    else:
      child.add(x[1])
    return child
  else:
    return x

macro `%*`*(x: expr): expr =
  ## Perform dict-like structure conversion into bson
  result = toBson(x)

template B*(key: string, val: Bson): expr =  ## Shortcut for _initBsonDocument
    initBsonDocument()(key, val)

template B*[T](key: string, values: seq[T]): expr =
    initBsonDocument()(key, values)

proc dbref*(refcol: string, refoid: Oid): Bson =
  ## Create new DBRef (database reference) MongoDB bson type
  return Bson(key: "", kind: BsonKindDBPointer, refcol: refcol, refoid: refoid)

proc undefined*(): Bson =
  ## Create new Bson 'undefined' value
  return Bson(key: "", kind: BsonKindUndefined)

proc null*(): Bson =
  ## Create new Bson 'null' value
  return Bson(key: "", kind: BsonKindNull)

proc minkey*(): Bson =
  ## Create new Bson value representing 'Min key' bson type
  return Bson(key: "", kind: BsonKindMinimumKey)

proc maxkey*(): Bson =
  ## Create new Bson value representing 'Max key' bson type
  return Bson(key: "", kind: BsonKindMaximumKey)

proc regex*(pattern: string, options: string): Bson =
  ## Create new Bson value representing Regexp bson type
  return Bson(key: "", kind: BsonKindRegexp, regex: pattern, options: options)

proc js*(code: string): Bson =
  ## Create new Bson value representing JavaScript code bson type
  return Bson(key: "", kind: BsonKindJSCode, valueCode: code)

proc bin*(bindata: string): Bson =
  ## Create new binary Bson object with 'generic' subtype
  return Bson(
    key: "", kind: BsonKindBinary, subtype: BsonSubtypeGeneric,
    valueGeneric: bindata
  )

proc binstr*(x: Bson): string =
  return x.valueGeneric

proc binuser*(bindata: string): Bson =
  ## Create new binray Bson object with 'user-defined' subtype
  return Bson(
    key: "", kind: BsonKindBinary, subtype: BsonSubtype.BsonSubtypeUserDefined,
    valueUserDefined: bindata
  )

proc geo*(loc: GeoPoint): Bson =
  ## Convert array of two floats into Bson as MongoDB Geo-Point.
  return Bson(
    key: "",
    kind: BsonKindArray,
    valueArray: @[loc[0].toBson(), loc[1].toBson()]
  )

proc `()`*(bs: Bson, key: string, val: Bson): Bson {.discardable.} =
  ## Add field to bson object
  result = bs
  if bs.kind == BsonKindDocument:
    if not isNil(val):
        var value: Bson = val
        value.key = key
        result.valueDocument.add(value)
    else:
        var value: Bson = null()
        value.key = key
        result.valueDocument.add(value)
  else:
    raise new(Exception)

proc `()`*[T](bs: Bson, key: string, values: seq[T]): Bson {.discardable.} =
    ## Add array field to bson object
    result = bs

    var arr: Bson = initBsonArray()

    arr.kind = BsonKindArray
    arr.valueArray = @[]
    arr.key = key

    var counter = 0
    for val in values.items():
        var tmpVal: Bson = val
        tmpVal.key = $counter
        arr.valueArray.add(tmpVal)
        inc(counter)

    result.valueDocument.add(arr)

proc add*[T](bs: Bson, value: T): Bson =
  result = bs
  var val: Bson = value
  val.key = $len(bs.valueArray)
  result.valueArray.add(val)

proc `[]`*(bs: Bson, key: string): Bson =
    ## Get Bson document field
    if bs.kind == BsonKindDocument:
        for item in bs.valueDocument:
            if item.key == key:
                return item
    else:
        raise new(Exception)

proc `[]=`*(bs: Bson, key: string, value: Bson) =
  ## Modify Bson document field
  if bs.kind == BsonKindDocument:
    for i in 0 .. < bs.valueDocument.len():
      if bs.valueDocument[i].key == key:
        bs.valueDocument[i] = value
        bs.valueDocument[i].key = key
        return
    var newValue: Bson = value
    newValue.key = key
    bs.valueDocument.add(newValue)
  else:
    raise new(Exception)

proc `[]`*(bs: Bson, key: int): Bson =
  ## Get Bson array item by index
  if bs.kind == BsonKindArray:
    return bs.valueArray[key]
  else:
    raise new(Exception)

proc `[]=`*(bs: Bson, key: int, value: Bson) =
  ## Modify Bson array element
  if bs.kind == BsonKindArray:
    bs.valueArray[key] = value

iterator items*(bs: Bson): Bson =
  ## Iterate overt Bson document or array fields
  if bs.kind == BsonKindDocument:
    for item in bs.valueDocument:
      yield item
  elif bs.kind == BsonKindArray:
    for item in bs.valueArray:
      yield item

proc contains*(bs: Bson, key: string): bool =
  ## Checks if Bson document has a specified field
  if bs.kind == BsonKindDocument:
    for field in bs.valueDocument.items():
      if key == field.key:
        return true
    return false
  else:
    return false

converter seqCharToString(x: openarray[char]): string =
  ## Converts sequence of chars to string
  result = newStringOfCap(len(x))
  for c in x: result = result & c

proc initBsonDocument*(bytes: string): Bson =
    ## Create new Bson document from byte stream
    let stream: Stream = newStringStream(bytes)
    discard stream.readInt32()   ## docSize
    var document: Bson = initBsonDocument()

    let parseBson = proc(s: Stream, doc: Bson): Bson =
        let kind: BsonKind = s.readChar()
        var name: TaintedString = ""
        discard s.readLine(name)
        case kind:
        of BsonKindDouble:
            return doc(name.string, s.readFloat64())
        of BsonKindStringUTF8:
            let valueString: string = s.readStr(s.readInt32() - 1)
            discard s.readChar()
            return doc(name.string, valueString)
        of BsonKindDocument:
            let ds: int32 = stream.peekInt32()
            var subdoc = initBsonDocument(s.readStr(ds))
            return doc(name.string, subdoc)
        of BsonKindArray:
            let ds: int32 = stream.peekInt32()
            var subdoc = initBsonDocument(s.readStr(ds))
            var subarr = initBsonArray()
            subarr.valueArray = subdoc.valueDocument
            return doc(name.string, subarr)
        of BsonKindBinary:
            let
                ds: int32 = s.readInt32()
                st: BsonSubtype = s.readChar().BsonSubtype
            case st:
            of BsonSubtypeMd5:
                return doc(name.string, cast[MD5Digest](s.readStr(ds).cstring))
            of BsonSubtypeGeneric:
                return doc(name.string, bin(s.readStr(ds)))
            of BsonSubtype.BsonSubtypeUserDefined:
                return doc(name.string, binuser(s.readStr(ds)))
            else:
                raise new(Exception)
        of BsonKindUndefined:
            return doc(name.string, undefined())
        of BsonKindOid:
            let valueOid: Oid = cast[Oid](s.readStr(12).cstring)
            return doc(name.string, valueOid)
        of BsonKindBool:
            return doc(name.string, if s.readChar() == 0.char: false else: true)
        of BsonKindTimeUTC:
            let timeUTC: Bson = Bson(key: name, kind: BsonKindTimeUTC, valueTime: fromSeconds(s.readInt64().float64 / 1000))
            return doc(name.string, timeUTC)
        of BsonKindNull:
            return doc(name.string, null())
        of BsonKindRegexp:
            return doc(name.string, regex(s.readLine().string(), seqCharToString(sorted(s.readLine().string, system.cmp))))
        of BsonKindDBPointer:
            let
              refcol: string = s.readStr(s.readInt32() - 1)
              refoid: Oid = cast[Oid](s.readStr(12).cstring)
            discard s.readChar()
            return doc(name.string, dbref(refcol, refoid))
        of BsonKindJSCode:
            let
              code: string = s.readStr(s.readInt32() - 1)
            discard s.readChar()
            return doc(name.string, js(code))
        of BsonKindInt32:
            return doc(name.string, s.readInt32())
        of BsonKindTimestamp:
            return doc(name.string, cast[BsonTimestamp](s.readInt64()))
        of BsonKindInt64:
            return doc(name.string, s.readInt64())
        of BsonKindMinimumKey:
            return doc(name.string, minkey())
        of BsonKindMaximumKey:
            return doc(name.string, maxkey())
        else:
            raise new(Exception)

    while stream.peekChar() != 0.char:
        document = parseBson(stream, document)

    return document

when isMainModule:
    echo "Testing nimongo/bson.nim module..."
    let oid = genOid()
    var bdoc: Bson = initBsonDocument()(
        "image", bin("12312l3jkalksjslkvdsdas"))(
        "balance", 1000.23)(
        "name", "John")(
        "someId", oid)(
        "someTrue", true)(
        "surname", "Smith")(
        "someNull", null())(
        "minkey", minkey())(
        "maxkey", maxkey())(
        "digest", "".toMd5())(
        "regexp-field", regex("pattern", "ismx"))(
        "undefined", undefined())(
        "someJS", js("function identity(x) {return x;}"))(
        "someRef", dbref("db.col", genOid()))(
        "userDefined", binuser("some-binary-data"))(
        "someTimestamp", BsonTimestamp(increment: 1, timestamp: 1))(
        "subdoc", initBsonDocument()(
            "salary", 500
        )(
        "array", @[%*{"string": "hello"},%*{"string" : "world"}]
        )
    )
    echo bdoc
    let bbytes = bdoc.bytes()
    let recovered = initBsonDocument(bbytes)
    echo "RECOVERED: ", recovered

    var bdoc2 = initBsonArray()
    bdoc2 = bdoc2.add(2)
    bdoc2 = bdoc2.add(2)
    echo bdoc2
