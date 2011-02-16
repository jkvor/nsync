-module(esync_utils).
-compile(export_all).

-include("esync.hrl").

rdb_len(Len, <<Type, Rest/binary>>, _IsEncoded) ->
    case ((Type band 16#C0) bsr 6) of
        ?REDIS_RDB_6BITLEN ->
            {ok, false, Type band 16#3F, Len-1, Rest};
        ?REDIS_RDB_ENCVAL ->
            {ok, true, Type band 16#3F, Len-1, Rest};
        ?REDIS_RDB_14BITLEN ->
            <<Next, Rest1/binary>> = Rest,
            {ok, false, ((Type band 16#3F) bsl 8) bor Next, Len-2, Rest1};
        _ ->
            <<Next:3, Rest1/binary>> = Rest,
            <<Val:4/unsigned-integer>> = <<Type, Next/binary>>,
            {ok, false, Val, Len-4, Rest1}
    end.

