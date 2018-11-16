# Package
description = "Pure Nim driver for MongoDB with support of synchronous and asynchronous I/O modes"
version     = "0.2"
license     = "MIT"
author      = "Rostyslav Dzinko <rostislav.dzinko@gmail.com>"

# Dependencies
requires "scram >= 0.1.2"

task test, "tests":
  exec "nim c -r tests/nimongotest.nim"
