-module(esync_utils).
-compile(export_all).

-include("esync.hrl").

rdb_len(_Len, <<>>, _IsEncoded) ->
    {error, eof};

rdb_len(Len, <<Type, Rest/binary>>, _IsEncoded) ->
    case ((Type band 16#C0) bsr 6) of
        ?REDIS_RDB_6BITLEN ->
            {ok, false, Type band 16#3F, Len-1, Rest};
        ?REDIS_RDB_ENCVAL ->
            {ok, true, Type band 16#3F, Len-1, Rest};
        ?REDIS_RDB_14BITLEN ->
            case Rest of
                <<Next, Rest1/binary>> ->
                    {ok, false, ((Type band 16#3F) bsl 8) bor Next, Len-2, Rest1};
                _ ->
                    {error, eof}
            end;
        _ ->
            case Rest of
                <<Next:3, Rest1/binary>> ->
                    case <<Type, Next/binary>> of
                        <<Val:4/unsigned-integer>> ->
                            {ok, false, Val, Len-4, Rest1};
                        _ ->
                            {error, eof}
                    end;
                _ ->
                    {error, eof}
            end
    end.

