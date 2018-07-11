## Naive module for performance-testing
import times

template timeIt*(name: string, p: untyped): untyped =
  ## Performs timing of the block call, and makes output into stdout.
  let timeStart = cpuTime()
  p
  echo name, ": ", formatFloat((cpuTime() - timeStart) * 1000000, precision=12), " μs"
