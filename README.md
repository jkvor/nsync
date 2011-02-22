# About

nsync is an Erlang application that acts as a redis slave. When the nsync process starts, it opens a tcp socket to the redis server and issues a "SYNC" command.  Redis asynchronously dumps the dataset to disk and transfers it to the nsync slave.  Once the rdb dump has been sent to the slave, subsequent commands are replicated using the redis text protocol. 

# Build

   $ ./rebar compile
 
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

    $ erl -pa ebin
    1> nsync:start_link().
    {ok,<0.33.0>}
    2> ets:tab2list(nsync).
    [{<<"foo">>,<<"monkey">>},
     {<<"bar:one">>,<<"seahorse">>},
     {<<"bar:two">>,<<"jellyfish">>},
     {<<"foo:one">>,<<"donkey">>}]

### Basic blocking sync

    $ erl -pa ebin
    1> nsync:start_link([{block, true}]).
    {ok,<0.33.0>}
    2> ets:tab2list(nsync).
    [{<<"foo">>,<<"monkey">>},
     {<<"bar:one">>,<<"seahorse">>},
     {<<"bar:two">>,<<"jellyfish">>},
     {<<"foo:one">>,<<"donkey">>}]
    
### Example1: Custom callback

see [example1.erl](https://github.com/JacobVorreuter/nsync/blob/master/src/examples/example1.erl)

    $ erl -pa ebin
    1> example1:sync().
    ok
    2> ets:tab2list(foo).
    [{<<"foo">>,<<"monkey">>},
     {<<"foo:one">>,<<"donkey">>}]
    3> ets:tab2list(bar).
    [{<<"bar:one">>,<<"seahorse">>},
     {<<"bar:two">>,<<"jellyfish">>}]

# Exception Handling

nsync re-establishes the tcp connection and re-issues the SYNC command when the socket is closed. A tuple of the form, {error, closed} is sent to the callback when the socket closes and the {load, eof} tuple is sent to the callback when the rdb sync is complete.
