# About

nsync is a redis master/slave replication client written in Erlang. When the nsync process starts, it opens a tcp socket connected to the master redis server and issues a "SYNC" command.  The redis master asynchronously dumps its dataset to disk and transfers it to the nsync slave as an rdb dump.  The master then streams subsequent updates to the slave using the redis text protocol.

# Build

   $ ./rebar compile
 
# Run

    $ erl -pa ebin
    $ nsync:start_link().

# Options

    nsync:start_link([Opt]).
    Opt = {host, Host} |
          {port, Port} |
          {pass, Pass} |
          {callback, Callback} |
          {block, Block} |
          {timeout, Timeout}
    Host = list() % redis master host 
    Port = integer() % redis master port
    Pass = list() % redis master password
    Block = boolean() % block calling pid during sync process
    Timeout = integer() % timeout of blocking sync process
    Callback = {M, F} | {M, F, A} | Fun
    M = atom()
    F = atom()
    A = list()
    Fun = fun()
