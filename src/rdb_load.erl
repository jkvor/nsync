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
                            lists:member(Vsn, [<<"0001">>,<<"0002">>,<<"0003">>,
                                               <<"0004">>,<<"0005">>,<<"0006">>])
                             orelse exit({error, vsn_not_supported}),
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

check_packet(<<"\r\n", Data/binary>>) ->
    {ok, Data};

check_packet(<<"\n", Data/binary>>) ->
    {ok, Data};

check_packet(Data) ->
    {ok, Data}.

read_line(Data) ->
    case binary:split(Data, <<"\r\n">>) of
        [_] -> {error, eof};
        [] -> {error, eof};
        [Line,Rest] ->
            {ok, Line, Rest}
    end.

parse_len(<<>>) ->
    {error, eof};

parse_len(<<"\n">>) ->
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

parse(?REDIS_EXPIRETIME_MS, Data, Callback) ->
    case Data of
        <<_Time:64/unsigned-integer, Rest/binary>> ->
            parse(Rest, Callback);
        _ ->
            {ok, <<?REDIS_EXPIRETIME_MS, Data/binary>>}
    end;
parse(?REDIS_EXPIRETIME_SEC, Data, Callback) ->
    case Data of
        <<_Time:32/unsigned-integer, Rest/binary>> ->
            parse(Rest, Callback);
        _ ->
            {ok, <<?REDIS_EXPIRETIME_SEC, Data/binary>>}
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
    nsync_utils:do_callback(Callback, [{load, Key, Val}]),
    ok.

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
                    exit({"Unknown RDB encoding type", Len})
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

rdb_load_object(?REDIS_ZMAP, Data) ->
    {ok, List, Rest} = parse_zmap_vals(Data),
    Dict = to_hash(List),
    {ok, Dict, Rest};

rdb_load_object(?REDIS_ZLIST, Data) ->
    parse_zlist_vals(Data);

rdb_load_object(?REDIS_INTSET, Data) ->
    parse_intset_vals(Data);

rdb_load_object(?REDIS_SSZLIST, Data) ->
    {ok, List, Rest} = parse_zlist_vals(Data),
    Set = to_sorted_set(List),
    {ok, Set, Rest};

rdb_load_object(?REDIS_HMAPZLIST, Data) ->
    {ok, List, Rest} = parse_zlist_vals(Data),
    Dict = to_hash(List),
    {ok, Dict, Rest};

rdb_load_object(_Type, _Data) ->
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


parse_zmap_vals(Data) ->
    {ok, Str, Rest1} = rdb_encoded_string_object(Data),
    <<_:8/unsigned-little, Rest/binary>> = Str,
    {ok, parse_zmap_entry(Rest), Rest1}.

parse_zmap_entry(<<255>>) ->
    [];
parse_zmap_entry(<<253, Len:32/little-unsigned, Entries/binary>>) ->
    <<Entry:Len/binary, Free, ToSkip/binary>> = Entries,
    <<_:Free/binary, Rest/binary>> = ToSkip,
    [maybe_int(Entry) | parse_zmap_entry(Rest)];
parse_zmap_entry(<<Len:8, Entries/binary>>) ->
    <<Entry:Len/binary, Free, ToSkip/binary>> = Entries,
    <<_:Free/binary, Rest/binary>> = ToSkip,
    [maybe_int(Entry) | parse_zmap_entry(Rest)].


maybe_int(Bin) ->
    try
        _ = list_to_integer(L = binary_to_list(Bin)),
        L
    catch
        error:badarg ->
            Bin
    end.

parse_zlist_vals(Data) ->
    {ok, Str, Rest1} = rdb_encoded_string_object(Data),
    <<_ZlBytes:32/little-unsigned,
      _ZlTail:32/little-unsigned,
      ZlLen:16/little-unsigned,
      Entries/binary>> = Str,
      {ok, parse_zlist_entries(ZlLen, Entries), Rest1}.

parse_zlist_entries(0, <<255>>) ->
    [];
parse_zlist_entries(Len, <<254:8/unsigned, _Prev:32, Entries/binary>>) ->
    {Entry, Rest} = parse_zlist_entry(Entries),
    [Entry | parse_zlist_entries(Len-1, Rest)];
parse_zlist_entries(Len, <<_Prev:8/unsigned, Entries/binary>>) ->
    {Entry, Rest} = parse_zlist_entry(Entries),
    [Entry | parse_zlist_entries(Len-1, Rest)].

%% String value with length less than or equal to 63 bytes (6 bits).
parse_zlist_entry(<<0:2, Len:6/little-unsigned, Entries/binary>>) ->
    <<Entry:Len/binary, Rest/binary>> = Entries,
    {Entry, Rest};
%% String value with length less than or equal to 16383 bytes (14 bits).
parse_zlist_entry(<<0:1,1:1, Len:14/little-unsigned, Entries/binary>>) ->
    <<Entry:Len/binary, Rest/binary>> = Entries,
    {Entry, Rest};
%% String value with length greater than or equal to 16384 bytes.
parse_zlist_entry(<<1:1,0:1,_:6, Len:32/little-unsigned, Entries/binary>>) ->
    <<Entry:Len/binary, Rest/binary>> = Entries,
    {Entry, Rest};
%% Read next 2 bytes as a 16 bit signed integer
parse_zlist_entry(<<1:1,1:1,0:2,_:4, Int:16/little-signed, Rest/binary>>) ->
    {integer_to_list(Int), Rest};
%% Read next 4 bytes as a 32 bit signed integer
parse_zlist_entry(<<1:1,1:1,0:1,1:1,_:4, Int:32/little-signed, Rest/binary>>) ->
    {integer_to_list(Int), Rest};
%% Read next 8 bytes as a 64 bit signed integer
parse_zlist_entry(<<1:1,1:1,1:1,0:1,_:4, Int:64/little-signed, Rest/binary>>) ->
    {integer_to_list(Int), Rest};
%% Read next 3 bytes as a 24 bit signed integer
parse_zlist_entry(<<1:1,1:1,1:1,1:1,0:4, Int:24/little-signed, Rest/binary>>) ->
    {integer_to_list(Int), Rest};
%% Read next byte as an 8 bit signed integer
parse_zlist_entry(<<1:1, 1:1, 1:1, 1:1, 1:1, 1:1, 1:1, 0:1, Int:8/little-signed, Rest/binary>>) ->
    {integer_to_list(Int), Rest};
%% immediate 4 bit integer. Unsigned integer from 0 to 12.
%% The encoded value is actually from 1 to 13 because 0000 and 1111 can not
%% be used, so 1 should be subtracted from the encoded 4 bit value to
%% obtain the right value
parse_zlist_entry(<<1:1,1:1,1:1,1:1, Val:4/little-unsigned, Rest/binary>>) ->
    {integer_to_list(Val-1), Rest}.

parse_intset_vals(Data) ->
    {ok, Str, Rest1} = rdb_encoded_string_object(Data),
    <<Encoding:32/little-unsigned, % byte size of integers
      Length:32/little-unsigned,
      Entries/binary>> = Str,
      {ok, parse_intset_entries(Encoding*8,Length,Entries), Rest1}.

parse_intset_entries(_Size, 0, <<>>) ->
    [];
parse_intset_entries(Size, N, Entries) ->
    <<Int:Size/little-signed, Rest/binary>> = Entries,
    [integer_to_list(Int) | parse_intset_entries(Size, N-1, Rest)].

to_hash(L) -> to_hash(L, dict:new()).

to_hash([], Dict) -> Dict;
to_hash([K,V|L], Dict) -> to_hash(L, dict:store(K,V,Dict)).

to_sorted_set(L) -> lists:sort(to_set(L)).

to_set([]) -> [];
to_set([Val,Weight|L]) -> [{Weight, Val} | to_set(L)].
