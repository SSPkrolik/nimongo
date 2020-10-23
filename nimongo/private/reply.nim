import ../bson
import ./errors

type StatusReply* = object  ## Database Reply
    ok*: bool
    n*: int
    err*: string
    inserted_ids*: seq[Bson]
    bson*: Bson

template parseReplyField(b: untyped, field: untyped, default: untyped, body: untyped): untyped =
  ## Take field from BSON. If field is missing and required than generate
  ## "ReplyFieldMissing" exception. If field is missing and not required
  ## than apply provided default value. If field exists than apply provided
  ## calculations body code.
  let b = reply[field]
  if b == nil.Bson:
    if isRequired:
      raise newException(ReplyFieldMissing, "Required field \"" & field & "\" missing in reply")
    else:
      result = default
  else:
    body

proc parseReplyOk(reply: Bson, isRequired: bool): bool {.raises: [ReplyFieldMissing, Exception]} =
  ## Parse "ok" field in database reply.
  parseReplyField(val, "ok", false):
    case val.kind
    of BsonKindDouble:
      result = val.toFloat64 == 1.0'f64
    else:
      result = val.toInt == 1

proc parseReplyN(reply: Bson, isRequired: bool): int {.raises: [ReplyFieldMissing, Exception]} =
  ## Parse "n" field in database reply.
  parseReplyField(val, "n", 0):
    case val.kind
    of BsonKindDouble:
      result = val.toFloat64.int
    else:
      result = val.toInt

proc parseReplyErrmsg(reply: Bson, isRequired: bool): string {.raises: [ReplyFieldMissing, Exception]} =
  ## Parse "errmsg" field in database reply.
  parseReplyField(val, "errmsg", ""):
    if val.kind == BsonKindStringUTF8:
      result = val.toString
    else:
      result = ""

proc toStatusReply*(reply: Bson, inserted_ids: seq[Bson] = @[]): StatusReply =
  ## Create StatusReply object from database reply BSON document and
  ## an optional list of OIDs.
  ## "ok" field is considered required. "n" and "errmsg" fields
  ## are optional and they are parsed if exist in reply
  result.bson = reply
  result.ok = parseReplyOk(reply, true)
  result.n = parseReplyN(reply, false)
  result.err = parseReplyErrmsg(reply, false)
  result.inserted_ids = inserted_ids

proc isReplyOk*(reply: Bson): bool =
  ## This function is useful if we would like to check only "ok" field
  ## in reply and do not like to create full StatusReply object. Field
  ## is considered required
  result = parseReplyOk(reply, true)

proc getReplyN*(reply: Bson): int =
  ## This function is useful if we would like to check only "n" field
  ## in reply and do not like to create full StatusReply object. Field
  ## is considered required
  result = parseReplyN(reply, true)

proc getReplyErrmsg*(reply: Bson): string =
  ## This function is useful if we would like to check only "errmsg" field
  ## in reply and do not like to create full StatusReply object. Field
  ## is considered required
  result = parseReplyErrmsg(reply, true)

converter toBool*(sr: StatusReply): bool = sr.ok
  ## If StatusReply.ok field is true = then StatusReply is considered
  ## to be successful. It is a convinience wrapper for the situation
  ## when we are not interested in no more status information than
  ## just a flag of success.
