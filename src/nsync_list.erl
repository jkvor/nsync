-module(nsync_list).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    ["lpush", "lpop", "rpush", "rpop"].

handle("lpush", [Key, Val], Tid) ->
    List = lookup(Tid, Key),
    ets:insert(Tid, {Key, [Val|List]});

handle("lpop", [Key], Tid) ->
    case ets:lookup(Tid, Key) of
        [{Key, [_Pop|Tail]}] ->
            ets:insert(Tid, {Key, Tail});
        _ ->
            ok
    end;

handle("rpush", [Key, Val], Tid) ->
    List = lookup(Tid, Key),
    ets:insert(Tid, {Key, List ++ [Val]});

handle("rpop", [Key], Tid) ->
    case lists:reverse(lookup(Tid, Key)) of
        [_Pop|Tail] ->
            ets:insert(Tid, {Key, lists:reverse(Tail)});
        [] ->
            ok
    end.

lookup(Tid, Key) ->
    case ets:lookup(Tid, Key) of
        [{Key, List}] -> List;
        [] -> []
    end.

