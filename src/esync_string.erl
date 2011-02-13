-module(esync_string).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    [<<"set">>].

handle(<<"set">>, [Key, Val], Tid) ->
    ets:insert(Tid, {Key, Val}).
