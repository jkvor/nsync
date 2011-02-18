## Examples

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

    $ rebar compile
    $ erl -pa ebin
    1> example:sync().
    ok
    finished loading rdb dump
    2> ets:tab2list(foo).
    [{<<"foo">>,<<"monkey">>},
     {<<"foo:one">>,<<"donkey">>}]
    3> ets:tab2list(bar).
    [{<<"bar:one">>,<<"seahorse">>},
     {<<"bar:two">>,<<"jellyfish">>}]

