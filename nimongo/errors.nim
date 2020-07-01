type
  CommunicationError* = object of CatchableError
    ## Raises on communication problems with MongoDB server

  NimongoError* = object of CatchableError
    ## Base exception for nimongo error (for simplifying error handling)  

  NotFound* = object of NimongoError
    ## Raises when querying of one documents returns empty result

  ReplyFieldMissing* = object of NimongoError
    ## Raises when reqired field in reply is missing
