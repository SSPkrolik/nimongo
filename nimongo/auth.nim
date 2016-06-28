type
    AuthenticationMethod* = enum
        NoAuth
        ScramSHA1                       ## +
        MongodbCr
        MongodbX509
        Kerberos                        ## Enterprise-only
        Ldap                            ## Enterprise-only
