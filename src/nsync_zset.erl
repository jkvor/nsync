-module(nsync_zset).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    ["zadd", "zrem"].

handle("zadd", [Key, Score, Member], Tid) ->
    Set = lookup(Tid, Key),
    Set1 = lists:filter(fun({_S,M}) -> M =/= Member end, Set),
    ets:insert(Tid, {Key, lists:usort([{Score, Member}|Set1])});

handle("zrem", [Key, Member], Tid) ->
    Set = lookup(Tid, Key),
    Set1 = lists:filter(fun({_S,M}) -> M =/= Member end, Set),
    ets:insert(Tid, {Key, Set1}).

lookup(Tid, Key) ->
    case ets:lookup(Tid, Key) of
        [{Key, Set}] -> Set;
        [] -> []
    end.
    
