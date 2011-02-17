-module(rdb_load).
-export([packet/3]).

-record(state, {buffer}).

-include("nsync.hrl").

packet(undefined, Data, Tid) ->
    {ok, _Len, Rest} = parse_len(Data),
    {ok, Rest1, Vsn} = parse_rdb_version(Rest),
    io:format("version ~s~n", [Vsn]),
    Vsn /= <<"0001">> andalso exit({error, vsn_not_supported}),
    packet(#state{buffer = <<>>}, Rest1, Tid);

packet(#state{buffer=Buffer}, Data, Tid) ->
    case parse(<<Buffer/binary, Data/binary>>, Tid) of
        {ok, Rest} ->
            #state{buffer = Rest};
        {error, eof} ->
            {error, eof}
    end.

parse_len(<<"$", Rest/binary>>) ->
    parse_len(Rest, []).

parse_len(<<"\r\n", Rest/binary>>, Acc) ->
    {ok, list_to_integer(lists:reverse(Acc)), Rest};

parse_len(<<Char, Rest/binary>>, Acc) ->
    parse_len(Rest, [Char|Acc]).

parse_rdb_version(<<"REDIS", Vsn:4/binary, Rest/binary>>) ->
    {ok, Rest, Vsn}.

parse(<<>>, _Tid) ->
    {ok, <<>>};

parse(Data, Tid) ->
    {ok, Type, Rest} = rdb_type(Data),
    parse(Type, Rest, Tid).

parse(?REDIS_EXPIRETIME, _Data, _Tid) ->
    exit("WTF is expire time?");

parse(?REDIS_EOF, <<>>, _Tid) ->
    {error, eof};

parse(?REDIS_SELECTDB, Data, Tid) ->
    case catch rdb_len(Data) of
        {'EXIT', {error, eof}} ->
            {ok, <<?REDIS_SELECTDB, Data/binary>>};
        {ok, _Enc, _Db, Rest} ->
            parse(Rest, Tid)
    end;

parse(Type, <<>>, _Tid) ->
    {ok, <<Type>>};

parse(Type, Data, Tid) ->
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
                    ets:insert(Tid, {Key, Val}),
                    parse(Rest1, Tid)
            end
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
