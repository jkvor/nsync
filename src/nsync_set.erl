-module(nsync_set).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    ["sadd", "srem", "smove"].

handle("sadd", [Key, Member], Tid) ->
    Set = lookup(Tid, Key),
    ets:insert(Tid, {Key, lists:usort([Member|Set])});

handle("srem", [Key, Member], Tid) ->
    Set = lookup(Tid, Key),
    ets:insert(Tid, {Key, lists:delete(Member, Set)});

handle("smove", [Source, Dest, Member], Tid) ->
    Set1 = lookup(Tid, Source),
    Set2 = lookup(Tid, Dest),
    NewSet1 = lists:delete(Member, Set1),
    NewSet2 = lists:usort([Member|Set2]),
    ets:insert(Tid, [{Source, NewSet1}, {Dest, NewSet2}]).

lookup(Tid, Key) ->
    case ets:lookup(Tid, Key) of
        [{Key, Set}] -> Set;
        [] -> []
    end.
