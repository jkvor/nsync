-module(esync_zset).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    [<<"set">>].

handle(<<"set">>, [Val], Tid) ->
    ok.
