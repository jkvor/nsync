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
    ets:insert(Tid, [{Source, NewSet1}, {Dest, NewSet2}]),
    NewSet1 == [] andalso ets:delete(Tid, Source).

lookup(Tid, Key) ->
    case ets:lookup(Tid, Key) of
        [{Key, Set}] -> Set;
        [] -> []
    end.
