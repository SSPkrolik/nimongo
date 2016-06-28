type StatusReply* = object  ## Database Reply
    ok*: bool
    n*: int
    err*: string

converter toBool*(sr: StatusReply): bool = sr.ok
  ## If StatusReply.ok field is true = then StatusReply is considered
  ## to be successful. It is a convinience wrapper for the situation
  ## when we are not interested in no more status information than
  ## just a flag of success.
