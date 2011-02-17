-module(nsync_hash).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    ["hdel", "hset", "hmset"].

handle("hdel", [Key, Field], Tid) ->
    case ets:lookup(Tid, Key) of
        [{Key, Hash}] ->
            ets:insert(Tid, {Key, dict:erase(Field, Hash)});
        [] ->
            ok
    end;

handle("hset", [Key, Field, Val], Tid) ->
    case ets:lookup(Tid, Key) of
        [{Key, Hash}] ->
            ets:insert(Tid, {Key, dict:store(Field, Val, Hash)});
        [] ->
            ets:insert(Tid, {Key, dict:from_list([{Field, Val}])})
    end;

handle("hmset", [Key|Tail], Tid) ->
    Hash =
        case ets:lookup(Tid, Key) of
            [{Key, Hash0}] -> Hash0;
            [] -> dict:new()
        end,
    Hash1 = lists:foldl(
        fun({K,V}, Acc) ->
            dict:store(K,V,Acc)
        end, Hash, flat_list_to_props(Tail)),
    ets:insert(Tid, {Key, Hash1}).

flat_list_to_props(List) ->
    flat_list_to_props(List, []).

flat_list_to_props([], Acc) -> lists:reverse(Acc);
flat_list_to_props([Key, Val | Tail], Acc) ->
    flat_list_to_props(Tail, [{Key, Val}|Acc]).
