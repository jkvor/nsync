%% Copyright (c) 2011 Jacob Vorreuter <jacob.vorreuter@gmail.com>
%% 
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%% 
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
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
