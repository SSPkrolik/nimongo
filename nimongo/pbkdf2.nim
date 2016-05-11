## PBKDF2

import sha1, hmac

proc stringWithData(p: pointer, bufferLen: uint32): string =
    result = newString(bufferLen)
    for i in 0'u ..< bufferLen:
       result[cast[int](i)] = cast[ptr char](cast[int](p) + i.int)[]

proc hmac_sha1_prf(keyPtr: pointer, keyLen: uint32,
         textPtr: pointer, textLen: uint32,
         randomPtr: pointer) {.cdecl.} =
    let r = hmac_sha1(stringWithData(keyPtr, keyLen), stringWithData(textPtr, textLen))
    for i, b in Sha1Digest(r):
       cast[ptr uint8](cast[int](randomPtr) + i)[] = b


## This function should generate a pseudo random octect stream
## of hLen bytes long (The value hLen is specified as an argument to pbkdf2
## and should be constant for any given prf function.) which is output in the buffer
## pointed to by randomPtr (the caller of this function is responsible for allocation
## of the buffer).
## The inputs to the pseudo random function are the first keyLen octets pointed
## to by keyPtr and the first textLen octets pointed to by textPtr.
## Both keyLen and textLen can have any nonzero value.
## A good prf would be a HMAC-SHA-1 algorithm where the keyPtr octets serve as
## HMAC's "key" and the textPtr octets serve as HMAC's "text".
type PRF = proc(keyPtr: pointer, keyLen: uint32,
         textPtr: pointer, textLen: uint32,
         randomPtr: pointer) {.cdecl.}

# Copy paste from opensource.apple.com
{.emit: """

typedef unsigned char UInt8;
typedef unsigned int UInt32;

typedef void (*PRF)(const void *keyPtr, UInt32 keyLen,
                    const void *textPtr, UInt32 textLen,
                    void *randomPtr);

/* Will write hLen bytes into dataPtr according to PKCS #5 2.0 spec.
   See: http://www.rsa.com/rsalabs/pubs/PKCS/html/pkcs-5.html for details.
   tempBuffer is a pointer to at least MAX (hLen, saltLen + 4) + hLen bytes. */
static void
F (PRF prf, UInt32 hLen,
   const void *passwordPtr, UInt32 passwordLen,
   const void *saltPtr, UInt32 saltLen,
   UInt32 iterationCount,
   UInt32 blockNumber,
   void *dataPtr,
   void *tempBuffer)
{
    UInt8 *inBlock, *outBlock, *resultBlockPtr;
    UInt32 iteration;
    outBlock = (UInt8*)tempBuffer;
    inBlock = outBlock + hLen;
    /* Set up inBlock to contain Salt || INT (blockNumber). */
    memcpy (inBlock, saltPtr, saltLen);

    inBlock[saltLen + 0] = (UInt8)(blockNumber >> 24);
    inBlock[saltLen + 1] = (UInt8)(blockNumber >> 16);
    inBlock[saltLen + 2] = (UInt8)(blockNumber >> 8);
    inBlock[saltLen + 3] = (UInt8)(blockNumber);

    /* Caculate U1 (result goes to outBlock) and copy it to resultBlockPtr. */
    resultBlockPtr = (UInt8*)dataPtr;
    prf (passwordPtr, passwordLen, inBlock, saltLen + 4, outBlock);
    memcpy (resultBlockPtr, outBlock, hLen);
    /* Calculate U2 though UiterationCount. */
    for (iteration = 2; iteration <= iterationCount; iteration++)
    {
        UInt8 *tempBlock;
        UInt32 byte;
        /* Swap inBlock and outBlock pointers. */
        tempBlock = inBlock;
        inBlock = outBlock;
        outBlock = tempBlock;
        /* Now inBlock conatins Uiteration-1.  Calclulate Uiteration into outBlock. */
        prf (passwordPtr, passwordLen, inBlock, hLen, outBlock);
        /* Xor data in dataPtr (U1 \xor U2 \xor ... \xor Uiteration-1) with
          outBlock (Uiteration). */
        for (byte = 0; byte < hLen; byte++)
            resultBlockPtr[byte] ^= outBlock[byte];
    }
}

""".}

proc F (prf: PRF, hLen: uint32,
    passwordPtr: pointer, passwordLen: uint32,
    saltPtr: pointer, saltLen: uint32,
    iterationCount, blockNumber: uint32,
    dataPtr, tempBuffer: pointer) {.importc, nodecl.}

proc pbkdf2 (prf: PRF, hLen: uint32, password, salt: string,
         iterationCount: uint32,
         dkPtr: pointer, dkLen: uint32,
         tempBuffer: pointer) =
    ## This function implements the PBKDF2 key derrivation algorithm described in
    ## http://www.rsa.com/rsalabs/pubs/PKCS/html/pkcs-5.html
    ## The output is a derived key of dkLen bytes which is written to the buffer
    ## pointed to by dkPtr.
    ## The caller should ensure dkPtr is at least dkLen bytes long.
    ## The Key is derived from password and from salt.  The algorithm used is
    ## desacribed in PKCS #5 version 2.0 and iterationCount iterations are performed.
    ## The argument prf is a pointer to a psuedo random number generator declared above.
    ## It should write exactly hLen bytes into its output buffer each time it is called.
    ## The argument tempBuffer should point to a buffer MAX (hLen, saltLen + 4) + 2 * hLen
    ## bytes long.  This buffer is used during the calculation for intermediate results.
    ## Security Considerations:
    ## The argument salt should be a pointer to a buffer of at least 8 random bytes
    ## (64 bits).  Thus saltLen should be >= 8.
    ## For each session a new salt should be generated.
    ## The value of iterationCount should be at least 1000 (one thousand).
    ## A good prf would be a HMAC-SHA-1 algorithm where the password serves as
    ## HMAC's "key" and the data serves as HMAC's "text".

    var completeBlocks = cast[int](dkLen) /% cast[int](hLen)
    var partialBlockSize : uint32 = dkLen mod hLen
    var dataPtr = dkPtr
    var blkBuffer = tempBuffer
    var blockNumber = 1
    # First cacluate all the complete hLen sized blocks required.
    while blockNumber <= completeBlocks:
        F (prf, hLen, password.cstring, password.len.uint32, salt.cstring, salt.len.uint32,
            iterationCount, blockNumber.uint32, dataPtr, cast[pointer](cast[uint](blkBuffer) + hLen))
        dataPtr = cast[pointer](cast[uint](dataPtr) + hLen)
        inc blockNumber

    # Finally if the requested output size was not an even multiple of hLen, calculate
    #   the final block and copy the first partialBlockSize bytes of it to the output.
    if partialBlockSize > 0'u:
        F (prf, hLen, password.cstring, password.len.uint32, salt.cstring, salt.len.uint32,
            iterationCount, blockNumber.uint32, blkBuffer, cast[pointer](cast[uint](blkBuffer) + hLen))
        copyMem(dataPtr, blkBuffer, partialBlockSize);

proc pbkdf2_hmac_sha1*(hLen: uint32, password, salt: string, iter: uint32): string =
    result = newString(hLen.int)
    var tempBuf = newString(int(max(hLen, uint32(salt.len + 4)) + 2'u32 * hLen))
    pbkdf2(hmac_sha1_prf, hLen, password, salt, iter, result.cstring, result.len.uint32, tempBuf.cstring)
