type
    CommunicationError* = object of Exception
        ## Raises on communication problems with MongoDB server

    NimongoError* = object of Exception
        ## Base exception for nimongo error (for simplifying error handling)  

    NotFound* = object of NimongoError
        ## Raises when querying of one documents returns empty result
