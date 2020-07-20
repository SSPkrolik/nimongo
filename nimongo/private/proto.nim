import ../bson

## Wire protocol codes
const OP_REPLY* =        1'i32       ## OP_REPLY operation code. Reply to a client request. responseTo is set.
const OP_UPDATE* =       2001'i32    ## OP_UPDATE operation code. Update document.
const OP_INSERT* =       2002'i32    ## OP_INSERT operation code. Insert new document.
const RESERVED* =        2003'i32    ## RESERVED operation code. Formerly used for OP_GET_BY_OID.
const OP_QUERY* =        2004'i32    ## OP_QUERY operation code. Query a collection.
const OP_GET_MORE* =     2005'i32    ## OP_GET_MORE operation code. Get more data from a query. See Cursors.
const OP_DELETE* =       2006'i32    ## OP_DELETE operation code. Delete documents.
const OP_KILL_CURSORS* = 2007'i32    ## OP_KILL_CURSORS operation code. Notify database that the client has finished with the cursor.
const OP_MSG* =          2013'i32    ## OP_MSG operation code. Send a message using the format introduced in MongoDB 3.6.

const HEADER_LENGTH* = 16'i32  ## Message Header size in bytes

proc buildMessageHeader*(messageLength, requestId, responseTo: int32, opCode: int32, res: var string) =
  ## Build Mongo message header as a series of bytes
  int32ToBytes(messageLength+HEADER_LENGTH, res)
  int32ToBytes(requestId, res)
  int32ToBytes(responseTo, res)
  int32ToBytes(opCode, res)

proc buildMessageQuery*(flags: int32, fullCollectionName: string, numberToSkip, numberToReturn: int32, res: var string) =
  ## Build Mongo query message
  int32ToBytes(flags, res)
  res &= fullCollectionName
  res &= char(0)
  int32ToBytes(numberToSkip, res)
  int32ToBytes(numberToReturn, res)

proc buildMessageMore*(fullCollectionName: string, cursorId: int64, numberToReturn: int32, res: var string) =
  ## Build Mongo get more message
  int32ToBytes(0'i32, res)
  res &= fullCollectionName
  res &= char(0)
  int32ToBytes(numberToReturn, res)
  int64ToBytes(cursorId, res)

proc buildMessageKillCursors*(cursorIds: seq[int64], res: var string) =
  ## Build Mongo kill cursors message
  let ncursorIds: int32 = cursorIds.len().int32
  if ncursorIds > 0:
    int32ToBytes(0'i32, res)
    int32ToBytes(ncursorIds, res)
    for cursorId in cursorIds:
      int64ToBytes(cursorId, res)
