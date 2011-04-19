# About

nsync is a redis master/slave replication client written in Erlang. When the nsync process starts, it opens a tcp socket connected to the master redis server and issues a "SYNC" command.  The redis master asynchronously dumps its dataset to disk and transfers it to the nsync slave as an rdb dump.  The master then streams subsequent updates to the slave using the redis text protocol.

# Build

   $ ./rebar compile
 
# Run

    $ erl -pa ebin
    $ nsync:start_link().

# Options

    nsync:start_link([Opt]).
    Opt = {ip, Ip} |
          {port, Port} |
          {pass, Pass} |
          {callback, Callback} |
          {block, Block} |
          {timeout, Timeout}
    Ip = list() % redis master IP
    Port = integer() % redis master port
    Pass = list() % redis master password
    Block = boolean() % block calling pid during sync process
    Timeout = integer() % timeout of blocking sync process
    Callback = {M, F} | {M, F, A} | Fun
    M = atom()
    F = atom()
    A = list()
    Fun = fun()
    
# Examples

### PREREQUISITE: Populating redis data

    $ ./redis-cli
    redis> flushdb
    OK
    redis> set foo monkey
    OK
    redis> set foo:one donkey
    OK
    redis> set bar:one seahorse
    OK
    redis> set bar:two jellyfish
    OK
    redis> keys *
    1) "foo:one"
    2) "foo"
    3) "bar:two"
    4) "bar:one"

### Basic async sync

nsync will perform the sync process in the background

    $ erl -pa ebin
    1> nsync:start_link().
    {ok,<0.33.0>}
    2> ets:tab2list(nsync:tid()).
    [{<<"foo">>,<<"monkey">>},
     {<<"bar:one">>,<<"seahorse">>},
     {<<"bar:two">>,<<"jellyfish">>},
     {<<"foo:one">>,<<"donkey">>}]

### Basic blocking sync

nsync will block the calling pid until the sync process has completed

    $ erl -pa ebin
    1> nsync:start_link([{block, true}]).
    {ok,<0.33.0>}
    2> ets:tab2list(nsync:tid()).
    [{<<"foo">>,<<"monkey">>},
     {<<"bar:one">>,<<"seahorse">>},
     {<<"bar:two">>,<<"jellyfish">>},
     {<<"foo:one">>,<<"donkey">>}]
    
# Callbacks

nsync supports custom callbacks. The callback module/function is specified in the nsync:start\_link options.

The callback module/function should handle any or all of the following tuples:

    {load, Key::binary(), Value::binary()} % load a key/value from the rdb dump
    {load, eof} % the {load, eof} tuple is sent to the callback when the rdb sync is complete.
    {error, closed} % nsync re-establishes the tcp connection and re-issues the SYNC command when the socket is closed. A tuple of the form, {error, closed} is sent to the callback when the socket closes.
    {error, {unhandled, Cmd::binary()}}
    {cmd, Cmd::binary(), Args::list()} % process an update command/args

The default callback function is nsync:default_callback()

### Example1: Custom callback

see [example1.erl](https://github.com/JacobVorreuter/nsync/blob/master/src/examples/example1.erl)

    $ erl -pa ebin
    1> example1:sync().
    ok
    2> ets:tab2list(nsync:tid(foo)).
    [{<<"foo">>,<<"monkey">>},
     {<<"foo:one">>,<<"donkey">>}]
    3> ets:tab2list(nsync:tid(bar)).
    [{<<"bar:one">>,<<"seahorse">>},
     {<<"bar:two">>,<<"jellyfish">>}]

