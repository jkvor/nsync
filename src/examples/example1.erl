-module(example1).
-export([sync/0, callback/3]).

sync() ->
    Foo = ets:new(foo, [public, named_table, set]),
    Bar = ets:new(bar, [public, named_table, set]),
    Opts = [
        {block, true},
        {callback, {?MODULE, callback, [Foo, Bar]}}
    ],
    {ok, _Pid} = nsync:start_link(Opts),
    ok.

callback(Foo, Bar, {load, Key, _Val}) ->
    case Key of
        <<"foo", _/binary>> -> Foo;
        <<"bar", _/binary>> -> Bar;
        _ -> undefined
    end;

callback(_Foo, _Bar, {load, eof}) ->
    ok;

callback(Foo, Bar, {cmd, _Cmd, Args}) ->
    case Args of
        [<<"foo", _/binary>> | _] -> Foo;
        [<<"bar", _/binary>> | _] -> Bar;
        _ -> undefined
    end. 
