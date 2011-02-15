-module(esync_set).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    ["sadd", "srem"].

handle("sadd", [Key, Member], Tid) ->
    Set = lookup(Tid, Key),
    ets:insert(Tid, {Key, lists:usort([Member|Set])});

handle("srem", [Key, Member], Tid) ->
    Set = lookup(Tid, Key),
    ets:insert(Tid, {Key, lists:delete(Member, Set)}).

lookup(Tid, Key) ->
    case ets:lookup(Tid, Key) of
        [{Key, Set}] -> Set;
        [] -> []
    end.
