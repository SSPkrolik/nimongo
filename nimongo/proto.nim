import bson

const OP_QUERY = 2004'i32  ## OP_QUERY operation code (wire protocol)

proc buildMessageHeader*(messageLength, requestId, responseTo: int32, res: var string) =
    ## Build Mongo message header as a series of bytes
    int32ToBytes(messageLength, res)
    int32ToBytes(requestId, res)
    int32ToBytes(responseTo, res)
    int32ToBytes(OP_QUERY, res)

proc buildMessageQuery*(flags: int32, fullCollectionName: string,
        numberToSkip, numberToReturn: int32, res: var string) =
    ## Build Mongo query message
    int32ToBytes(flags, res)
    res &= fullCollectionName
    res &= char(0)
    int32ToBytes(numberToSkip, res)
    int32ToBytes(numberToReturn, res)