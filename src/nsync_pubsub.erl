-module(nsync_pubsub).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    ["publish"].

handle("publish", _Args, _Tid) ->
    ok.
