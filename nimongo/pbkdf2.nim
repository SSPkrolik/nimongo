{.passL: "-lcrypto"}
## PBKDF2

proc PKCS5_PBKDF2_HMAC_SHA1*(password: pointer, passlen: uint, salt: pointer, saltlen: uint, iterations: uint, keylen: uint, k: pointer): cint {.importc, header: "<openssl/evp.h>", nodecl.}
proc EVP_sha1*(): pointer {.importc, header: "<openssl/evp.h>".}
proc HMAC*(hmac: pointer, key: pointer, keylen: cint, data: pointer, datalen: cint, xuinia1: pointer=nil, xuinia2: pointer=nil): cstring {.importc, header:"<openssl/hmac.h>", nodecl.}
