{.passL: "-lcrypto"}
## PBKDF2

proc PKCS5_PBKDF2_HMAC_SHA1*(password: pointer, passlen: uint, salt: pointer, saltlen: uint, iterations: uint, keylen: uint, k: pointer): cint {.importc.}
