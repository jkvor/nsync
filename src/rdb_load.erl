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
-module(rdb_load).
-export([packet/3]).

-record(state, {first = true, buffer = <<>>}).

-include("nsync.hrl").

packet(State, Data, Callback) when State == undefined orelse State#state.first == true ->
    Data1 =
        case State of
            undefined -> Data;
            #state{buffer=Buffer} -> <<Buffer/binary, Data/binary>>
        end,
    case check_packet(Data1) of
        {ok, line, Line} ->
            {error, Line};
        {ok, Rest} ->
            case parse_len(Rest) of
                {ok, _Len, Rest1} ->
                    case parse_rdb_version(Rest1) of 
                        {ok, Rest2, Vsn} ->
                            Vsn /= <<"0001">> andalso exit({error, vsn_not_supported}),
                            packet(#state{buffer = <<>>, first = false}, Rest2, Callback);
                        {error, eof} ->
                            #state{buffer = Data}
                    end;
                {error, eof} ->
                    #state{buffer = Data}
            end;
        {error, eof} ->
            #state{buffer = Data}
    end;

packet(#state{buffer=Buffer}, Data, Callback) ->
    case parse(<<Buffer/binary, Data/binary>>, Callback) of
        {ok, Rest} ->
            #state{buffer = Rest, first = false};
        {eof, Rest} ->
            {eof, Rest}
    end.

check_packet(<<Char, _/binary>> = Data) when Char == $-; Char == $+ ->
    case read_line(Data) of
        {ok, Line, <<>>} ->
            {ok, line, Line};
        {ok, _Line, Rest} ->
            {ok, Rest};
        {error, eof} ->
            {error, eof}
    end;

check_packet(Data) ->
    {ok, Data}.

read_line(Data) ->
    read_line(Data, <<>>).

read_line(<<"\r\n", Rest/binary>>, Acc) ->
    {ok, Acc, Rest};

read_line(<<>>, _Acc) ->
    {error, eof};

read_line(<<Char, Rest/binary>>, Acc) ->
    read_line(Rest, <<Acc/binary, Char>>).

parse_len(<<>>) ->
    {error, eof};

parse_len(<<"$", Rest/binary>>) ->
    case read_line(Rest) of
        {ok, Line, Rest1} ->
            {ok, list_to_integer(binary_to_list(Line)), Rest1};
        {error, eof} ->
            {error, eof}
    end.

parse_rdb_version(<<"REDIS", Vsn:4/binary, Rest/binary>>) ->
    {ok, Rest, Vsn};

parse_rdb_version(_) ->
    {error, eof}.

parse(<<>>, _Callback) ->
    {ok, <<>>};

parse(Data, Callback) ->
    {ok, Type, Rest} = rdb_type(Data),
    parse(Type, Rest, Callback).

parse(?REDIS_EXPIRETIME, Data, Callback) ->
    case Data of
        <<_Time:32/unsigned-integer, Rest/binary>> ->
            parse(Rest, Callback);
        _ ->
            {ok, <<?REDIS_EXPIRETIME, Data/binary>>}
    end;

parse(?REDIS_EOF, Rest, _Callback) ->
    {eof, Rest};

parse(?REDIS_SELECTDB, Data, Callback) ->
    case catch rdb_len(Data) of
        {'EXIT', {error, eof}} ->
            {ok, <<?REDIS_SELECTDB, Data/binary>>};
        {ok, _Enc, _Db, Rest} ->
            parse(Rest, Callback)
    end;

parse(Type, <<>>, _Callback) ->
    {ok, <<Type>>};

parse(Type, Data, Callback) ->
    case catch rdb_string_object(Data) of
        {'EXIT', {error, eof}} ->
            {ok, <<Type, Data/binary>>};
        {'EXIT', Err} ->
            exit(Err);
        {ok, Key, Rest} ->
            case catch rdb_load_object(Type, Rest) of
                {'EXIT', {error, eof}} ->
                    {ok, <<Type, Data/binary>>};
                {'EXIT', Err} ->
                    exit(Err);
                {ok, Val, Rest1} ->
                    write(Callback, Key, Val),
                    parse(Rest1, Callback)
            end
    end.

write(Callback, Key, Val) ->
    case nsync_utils:do_callback(Callback, [{load, Key, Val}]) of
        undefined ->
            ok;
        Name ->
            Tid = nsync_utils:lookup_write_tid(nsync_tids, Name),
            ets:insert(Tid, {Key, Val})
    end.

rdb_type(<<Type, Rest/binary>>) ->
    {ok, Type, Rest};

rdb_type(<<>>) ->
    exit({error, eof}).

rdb_len(<<>>) ->
    exit({error, eof});

rdb_len(<<Type, Rest/binary>>) ->
    case ((Type band 16#C0) bsr 6) of
        ?REDIS_RDB_6BITLEN ->
            {ok, false, Type band 16#3F, Rest};
        ?REDIS_RDB_ENCVAL ->
            {ok, true, Type band 16#3F, Rest};
        ?REDIS_RDB_14BITLEN ->
            case Rest of
                <<Next, Rest1/binary>> ->
                    {ok, false, ((Type band 16#3F) bsl 8) bor Next, Rest1};
                _ ->
                    exit({error, eof})
            end;
        _ ->
            case Rest of
                <<Next:3, Rest1/binary>> ->
                    case <<Type, Next/binary>> of
                        <<Val:4/unsigned-integer>> ->
                            {ok, false, Val, Rest1};
                        _ ->
                            exit({error, eof})
                    end;
                _ ->
                    exit({error, eof})
            end
    end.

rdb_string_object(Data) ->
    rdb_generic_string_object(Data, false). 

rdb_encoded_string_object(Data) ->
    rdb_generic_string_object(Data, true).

rdb_generic_string_object(Data, _Encode) ->
    {ok, Enc, Len, Rest} = rdb_len(Data),
    case Enc of
        true ->
            case Len of
                ?REDIS_RDB_ENC_INT8 ->
                    rdb_integer_object(Len, Rest);
                ?REDIS_RDB_ENC_INT16 ->
                    rdb_integer_object(Len, Rest);
                ?REDIS_RDB_ENC_INT32 ->
                    rdb_integer_object(Len, Rest);
                ?REDIS_RDB_ENC_LZF ->
                    rdb_lzf_string_object(Rest);
                _ ->
                    exit("Unknown RDB encoding type")
            end;
        false ->
            case Rest of
                <<Str:Len/binary, Rest1/binary>> ->
                    {ok, Str, Rest1};
                _ ->
                    exit({error, eof})
            end
    end.

rdb_integer_object(?REDIS_RDB_ENC_INT8, <<Char, Rest/binary>>) ->
    {ok, integer_to_list(Char), Rest};

rdb_integer_object(?REDIS_RDB_ENC_INT8, _) ->
    exit({error, eof});

rdb_integer_object(?REDIS_RDB_ENC_INT16, <<A, B, Rest/binary>>) ->
    {ok, integer_to_list(A bor (B bsl 8)), Rest};

rdb_integer_object(?REDIS_RDB_ENC_INT16, _) ->
    exit({error, eof});

rdb_integer_object(?REDIS_RDB_ENC_INT32, <<A,B,C,D, Rest/binary>>) ->
    {ok, integer_to_list(A bor (B bsl 8) bor (C bsl 16) bor (D bsl 24)), Rest};

rdb_integer_object(?REDIS_RDB_ENC_INT32, _) ->
    exit({error, eof});

rdb_integer_object(_Type, _Data) ->
    exit("Unknown RDB integer encoding type").

%% TODO: parse doubles
rdb_double_value(Data) ->
    rdb_encoded_string_object(Data).

rdb_lzf_string_object(Data) ->
    {ok, _Enc1, LzfLen, Rest} = rdb_len(Data),
    {ok, _Enc2, _UncompLen, Rest1} = rdb_len(Rest),
    case Rest1 of
        <<LzfEnc:LzfLen/binary, Rest2/binary>> ->
            case (catch lzf:decompress(LzfEnc)) of
                {'EXIT', _Err} ->
                    error_logger:error_msg("failed lzf_decompress(~p)~n", [LzfEnc]),
                    {ok, <<"">>, Rest2};
                Str ->
                    {ok, Str, Rest2}
            end;
        _ ->
            exit({error, eof})
    end.

rdb_load_object(_Type, <<>>) ->
    exit({error, eof});    

rdb_load_object(?REDIS_STRING, Data) ->
    rdb_encoded_string_object(Data);

rdb_load_object(?REDIS_LIST, Data) ->
    {ok, _Enc, Size, Rest} = rdb_len(Data),
    parse_list_vals(Size, Rest, []);

rdb_load_object(?REDIS_SET, Data) ->
    {ok, _Enc, Size, Rest} = rdb_len(Data),
    parse_list_vals(Size, Rest, []);

rdb_load_object(?REDIS_ZSET, Data) ->
    {ok, _Enc, Size, Rest} = rdb_len(Data),
    parse_zset_vals(Size, Rest, []);

rdb_load_object(?REDIS_HASH, Data) ->
    {ok, _Enc, Size, Rest} = rdb_len(Data),
    parse_hash_props(Size, Rest, dict:new());

rdb_load_object(_Type, _Data) ->
    io:format("unknown object type: ~p~n", [_Type]),
    exit("Unknown object type").

parse_list_vals(0, Rest, Acc) ->
    {ok, lists:reverse(Acc), Rest};

parse_list_vals(Size, Rest, Acc) ->
    {ok, Str, Rest1} = rdb_encoded_string_object(Rest),
    parse_list_vals(Size-1, Rest1, [Str|Acc]).

parse_zset_vals(0, Rest, Acc) ->
    {ok, lists:sort(Acc), Rest};

parse_zset_vals(Size, Rest, Acc) ->
    {ok, Str, Rest1} = rdb_encoded_string_object(Rest),
    {ok, Score, Rest2} = rdb_double_value(Rest1),
    parse_zset_vals(Size-1, Rest2, [{Score, Str}|Acc]).

parse_hash_props(0, Rest, Acc) ->
    {ok, Acc, Rest};

parse_hash_props(Size, Rest, Acc) ->
    {ok, Key, Rest1} = rdb_encoded_string_object(Rest),
    {ok, Val, Rest2} = rdb_encoded_string_object(Rest1),
    parse_hash_props(Size-1, Rest2, dict:store(Key, Val, Acc)).
