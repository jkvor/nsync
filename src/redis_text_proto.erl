-module(redis_text_proto).
-export([parse_commands/3]).

-include("nsync.hrl").

parse_commands(<<>>, _Callback, _Map) ->
    {ok, <<>>};

parse_commands(<<"*", Rest/binary>>, Callback, Map) ->
    case parse_num(Rest, <<>>) of
        {ok, Num, Rest1} ->
            case parse_num_commands(Rest1, Num, []) of 
                {ok, [Cmd|Args], Rest2} ->
                    dispatch_cmd(Cmd, Args, Callback, Map),
                    parse_commands(Rest2, Callback, Map);
                {error, eof} ->
                    {ok, <<"*", Rest/binary>>}
            end;
        {error, eof} ->
            {ok, <<"*", Rest/binary>>}
    end.

dispatch_cmd(Cmd, Args, Callback, Map) ->
    Cmd1 = string:to_lower(binary_to_list(Cmd)),
    case dict:find(Cmd1, Map) of
        {ok, Mod} ->
            case nsync_utils:do_callback(Callback, [{cmd, Cmd1, Args}]) of
                undefined ->
                    ok;
                Tid -> 
                    Mod:handle(Cmd1, Args, Tid)
            end;
        error ->
            catch nsync_utils:do_callback(Callback, [{error, {unhandled_command, Cmd1}}]) 
    end.

parse_num(<<"\r\n", Rest/binary>>, Acc) ->
    {ok, list_to_integer(binary_to_list(Acc)), Rest};

parse_num(<<"\r", _Rest/binary>>, _Acc) ->
    {error, eof};

parse_num(<<>>, _Acc) ->
    {error, eof};
    
parse_num(<<Char, Rest/binary>>, Acc) ->
    parse_num(Rest, <<Acc/binary, Char>>).

parse_num_commands(Rest, 0, Acc) ->
    {ok, lists:reverse(Acc), Rest};

parse_num_commands(<<"$", Rest/binary>>, Num, Acc) ->
    case parse_num(Rest, <<>>) of
        {ok, Size, Rest1} ->
            case read_string(Size, Rest1) of
                {ok, Cmd, Rest2} ->
                    parse_num_commands(Rest2, Num-1, [Cmd|Acc]);
                {error, eof} ->
                    {error, eof}
            end;
        {error, eof} ->
            {error, eof}
    end.

read_string(Size, Data) ->
    case Data of
        <<Cmd:Size/binary, "\r\n", Rest/binary>> ->
            {ok, Cmd, Rest};
        _ ->
            {error, eof}
    end.
