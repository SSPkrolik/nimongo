# This module implements MongoDB WriteConcern support
import strutils

import bson

const Journaled*: bool = true

type WriteConcern* = Bson

proc writeConcern*(w: int32, j: bool, wtimeout: int = 0): WriteConcern =
    ## Custom write concern creation
    result = %*{"w": w, "j": j}
    if wtimeout > 0:
        result["wtimeout"] = wtimeout.toBson()

proc writeConcernDefault*(): Bson = 
    ## Default value for write concern at MongoDB server
    writeConcern(1, not Journaled)

proc writeConcernMajority*(wtimeout: int = 0): WriteConcern =
    ## Majority of replica set members must approve
    ## that write operation was successful
    result = %*{"w": "majority", "j": Journaled}
    if wtimeout > 0:
        result["wtimeout"] = wtimeout.toBson()
