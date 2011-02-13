-module(esync_hash).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    [<<"hset">>].

handle(<<"hset">>, [Key, Field, Val], Tid) ->
    ok.
