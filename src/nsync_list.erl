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
-module(nsync_list).
-export([command_hooks/0, handle/3]).

command_hooks() ->
    ["lpush", "lpop", "rpush", "rpop", "lrem"].

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
    end;

handle("lrem", [Key, Count, Val], Tid) ->
    List = lookup(Tid, Key),
    N = list_to_integer(binary_to_list(Count)),
    ets:insert(Tid, {Key, lrem(List, N, Val)}).


lookup(Tid, Key) ->
    case ets:lookup(Tid, Key) of
        [{Key, List}] -> List;
        [] -> []
    end.

%%===================================================================
%% internal functions
%%===================================================================

%% Redis lrem command, which removes the first count occurrences of
%% elements equal to value from the list:
%%  count > 0: Remove elements equal to value moving from head to tail.
%%  count < 0: Remove elements equal to value moving from tail to head.
%%  count = 0: Remove all elements equal to value.
%%
lrem(List, Count, Val) when Count == 0 ->
    lists:dropwhile(fun(Elem) -> Elem =:= Val end, List);
lrem(List, Count, Val) when Count > 0 ->
    {_, NewList} = lists:foldl(
                        fun(Elem, Acc) ->
                            {N, L} = Acc,
                            case {N, Elem =:= Val} of
                                {0, _}      -> {N, [Elem | L]};
                                {_, true}   -> {N - 1, L};
                                _           -> {N, [Elem | L]}
                            end
                        end, {Count, []}, List),
    lists:reverse(NewList);
lrem(List, Count, Val) when Count < 0 ->
    {_, NewList} = lists:foldr(
                        fun(Elem, Acc) ->
                            {N, L} = Acc,
                            case {N, Elem =:= Val} of
                                {0, _}      -> {N, [Elem | L]};
                                {_, true}   -> {N + 1, L};
                                _           -> {N, [Elem | L]}
                            end
                        end, {Count, []}, List),
    NewList.
