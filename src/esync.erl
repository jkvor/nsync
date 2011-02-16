-module(esync).
-behaviour(gen_server).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% API
-export([start_link/0, start_link/1, start_link/2]).

-include("esync.hrl").

-record(state, {tid, socket, opts, state, len_remaining, buffer, map}).

-define(TIMEOUT, 30000).

%%====================================================================
%% API functions
%%====================================================================
start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

start_link(Name, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Opts], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Opts]) ->
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 6379),
    Auth = proplists:get_value(auth, Opts),
    Tid  = proplists:get_value(tid, Opts, ?MODULE),
    case open_socket(Host, Port) of
        {ok, Socket} ->
            case authenticate(Socket, Auth) of
                ok ->
                    Map = init_map(),
                    init_table(Tid),
                    init_sync(Socket),
                    inet:setopts(Socket, [{active, once}]),
                    {ok, #state{
                        tid=Tid,
                        socket=Socket,
                        opts=Opts,
                        state=loading,
                        buffer = <<>>,
                        map=Map
                    }};
                Error ->
                    {stop, Error}
            end;
        Error ->
            {stop, Error}
    end.

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, #state{tid=Tid,
                                        socket=Socket,
                                        state=loading,
                                        len_remaining=undefined}=State) ->
    io:format("~p~n", [Data]),
    {ok, Len, Rest} = parse_len(Data),
    io:format("start loading ~w chars~n", [Len]),
    {ok, Len1, Rest1, Vsn} = parse_rdb_version(Len, Rest),
    io:format("version ~s~n", [Vsn]),
    Vsn /= <<"0001">> andalso exit({error, vsn_not_supported}),
    {ok, Len2, Rest2, NewState} = parse_keys(Len1, Rest1, Tid),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{len_remaining=Len2, state=NewState, buffer=Rest2}};

handle_info({tcp, Socket, Data}, #state{tid=Tid,
                                        socket=Socket,
                                        state=loading,
                                        buffer=Buffer,
                                        len_remaining=Len}=State) ->
    io:format("~p~n", [Data]),
    {ok, Len1, Rest1, NewState} = parse_keys(Len, <<Buffer/binary, Data/binary>>, Tid),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{len_remaining=Len1, state=NewState, buffer=Rest1}};

handle_info({tcp, Socket, Data}, #state{tid=Tid,
                                        socket=Socket,
                                        buffer=Buffer,
                                        map=Map}=State) ->
    {ok, Rest} = parse_commands(<<Buffer/binary, Data/binary>>, Tid, Map),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{buffer=Rest}};

handle_info({tcp_closed, _}, #state{socket=Socket}=State) ->
    io:format("connection closed~n"),
    catch gen_tcp:close(Socket),
    {stop, normal, State};

handle_info({tcp_error, _ ,_}, #state{socket=Socket}=State) ->
    io:format("tcp_error~n"),
    catch gen_tcp:close(Socket),
    {stop, normal, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% internal functions
%%====================================================================
open_socket(Host, Port) when is_list(Host), is_integer(Port) ->
    gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}]);

open_socket(_Host, _Port) ->
    {error, invalid_host_or_port}.

authenticate(_Socket, undefined) ->
    ok;

authenticate(Socket, Auth) ->
    case gen_tcp:send(Socket, [<<"AUTH ">>, Auth, <<"\r\n">>]) of
        ok ->
            case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
                {ok, <<"OK\r\n">>} ->
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

init_map() ->
    Mods = [esync_string, esync_list, esync_set, esync_zset, esync_hash],
    lists:foldl(fun(Mod, Acc) ->
        lists:foldl(fun(Cmd, Acc1) ->
            io:format("loading ~p:~p~n", [Mod, Cmd]),
            dict:store(Cmd, Mod, Acc1)
        end, Acc, Mod:command_hooks())
    end, dict:new(), Mods).

init_table(Tid) ->
    case ets:info(Tid, protection) of
        undefined ->
            ets:new(Tid, [protected, named_table, set]);
        public ->
            Tid;
        _ ->
            exit({error, cannot_write_to_table})
    end.

init_sync(Socket) ->
    gen_tcp:send(Socket, <<"SYNC\r\n">>).

parse_len(<<"$", Rest/binary>>) ->
    parse_len(Rest, []).

parse_len(<<"\r\n", Rest/binary>>, Acc) ->
    {ok, list_to_integer(lists:reverse(Acc)), Rest};

parse_len(<<Char, Rest/binary>>, Acc) ->
    parse_len(Rest, [Char|Acc]).

parse_rdb_version(Len, <<"REDIS", Vsn:4/binary, Rest/binary>>) ->
    {ok, Len-9, Rest, Vsn}.

parse_keys(1, <<?REDIS_EOF>>, _Tid) ->
    {ok, 0, <<>>, up};

parse_keys(Len, <<?REDIS_SELECTDB, Rest/binary>>, Tid) ->
    {ok, _Enc, _Db, Len1, Rest1} = esync_utils:rdb_len(Len-1, Rest, undefined),
    parse_keys(Len1, Rest1, Tid);

parse_keys(_Len, <<?REDIS_EXPIRETIME, _Rest/binary>>, _Tid) ->
    exit("WTF is expire time?");

parse_keys(Len, <<?REDIS_STRING, Rest/binary>> = Data, Tid) ->
    case parse_string(Len-1, Rest) of
        {ok, Key, Len1, Rest1} ->
            case parse_string(Len1, Rest1) of
                {ok, Val, Len2, Rest2} ->
                    ets:insert(Tid, {Key, Val}),
                    parse_keys(Len2, Rest2, Tid);
                {error, eof} ->
                    {ok, Len, Data, loading}
            end;
        {error, eof} ->
            {ok, Len, Data, loading}
    end;

parse_keys(Len, <<?REDIS_LIST, Rest/binary>> = Data, Tid) ->
    case parse_string(Len-1, Rest) of
        {ok, Key, Len1, Rest1} ->
            case esync_utils:rdb_len(Len1, Rest1, undefined) of
                {ok, _Enc, Size, Len2, Rest2} ->
                    case parse_list_vals(Len2, Size, Rest2, []) of 
                        {ok, Vals, Len3, Rest3} ->
                            ets:insert(Tid, {Key, Vals}),
                            parse_keys(Len3, Rest3, Tid);
                        {error, eof} ->
                            {ok, Len, Data, loading}
                    end;
                {error, eof} ->
                    {ok, Len, Data, loading}
            end;
        {error, eof} ->
            {ok, Len, Data, loading}
    end;

parse_keys(Len, <<?REDIS_SET, Rest/binary>> = Data, Tid) ->
    case parse_string(Len-1, Rest) of
        {ok, Key, Len1, Rest1} ->
            case esync_utils:rdb_len(Len1, Rest1, undefined) of
                {ok, _Enc, Size, Len2, Rest2} ->
                    case parse_list_vals(Len2, Size, Rest2, []) of
                        {ok, Vals, Len3, Rest3} ->
                            ets:insert(Tid, {Key, Vals}),
                            parse_keys(Len3, Rest3, Tid);
                        {error, eof} ->
                            {ok, Len, Data, loading}
                    end;
                {error, eof} ->
                    {ok, Len, Data, loading}
              end;
        {error, eof} ->
              {ok, Len, Data, loading}
    end;

parse_keys(Len, <<?REDIS_ZSET, Rest/binary>> = Data, Tid) ->
    case parse_string(Len-1, Rest) of
        {ok, Key, Len1, Rest1} ->
            case esync_utils:rdb_len(Len1, Rest1, undefined) of 
                {ok, _Enc, Size, Len2, Rest2} ->
                    case parse_zset_vals(Len2, Size, Rest2, []) of
                        {ok, Vals, Len3, Rest3} ->
                            ets:insert(Tid, {Key, Vals}),
                            parse_keys(Len3, Rest3, Tid);    
                        {error, eof} ->
                            {ok, Len, Data, loading}
                    end;
                {error, eof} ->
                    {ok, Len, Data, loading}
            end;
        {error, eof} ->
            {ok, Len, Data, loading}
    end;

parse_keys(Len, <<?REDIS_HASH, Rest/binary>> = Data, Tid) ->
    case parse_string(Len-1, Rest) of
        {ok, Key, Len1, Rest1} ->
            case esync_utils:rdb_len(Len1, Rest1, undefined) of
                {ok, _Enc, Size, Len2, Rest2} ->
                    case parse_hash_props(Len2, Size, Rest2, []) of
                        {ok, Props, Len3, Rest3} ->
                            ets:insert(Tid, {Key, Props}),
                            parse_keys(Len3, Rest3, Tid);
                        {error, eof} ->
                            {ok, Len, Data, loading}
                    end;
                {error, eof} ->
                    {ok, Len, Data, loading}
            end;
        {error, eof} ->
            {ok, Len, Data, loading}
    end.

parse_string(Len, Data) ->
    io:format("parse_string ~w, ~p~n", [Len, Data]),
    case esync_utils:rdb_len(Len, Data, undefined) of
        {error, eof} ->
            {error, eof};
        {ok, Enc, Size, Len1, Rest} ->
            io:format("enc=~p, type=~p~n", [Enc, Size]),
            case Enc of
                true ->
                    case lists:member(Size, [?REDIS_RDB_ENC_INT8, ?REDIS_RDB_ENC_INT16, ?REDIS_RDB_ENC_INT32]) of
                        true ->
                            parse_integer_obj(Len1, Size, Rest);
                        false ->
                            case Size of
                                ?REDIS_RDB_ENC_LZF ->
                                    case Rest of
                                        <<LzfLen, Rest1/binary>> ->
                                            case Rest1 of
                                                <<_UncompLen, Rest2/binary>> ->
                                                    case Rest2 of
                                                        <<LzfEnc:LzfLen/binary, Rest3/binary>> ->
                                                            case (catch lzf:decompress(LzfEnc)) of
                                                                {'EXIT', _Err} ->
                                                                    error_logger:error_msg("failed lzf_decompress(~s)~n", [LzfEnc]),
                                                                    {ok, "", (Len1-2)-LzfLen, Rest3};
                                                                Str ->
                                                                    {ok, Str, (Len1-2)-LzfLen, Rest3}
                                                            end;
                                                        _ ->
                                                            {error, eof}
                                                    end;
                                                _ ->
                                                    {error, eof}
                                            end;
                                        _ ->
                                            {error, eof}
                                    end;
                                _ ->
                                    exit("Unknown RDB encoding type")
                            end
                    end;
                false ->
                    case Rest of
                        <<Str:Size/binary, Rest1/binary>> ->
                            {ok, Str, Len1-Size, Rest1};
                        _ ->
                            {error, eof}
                    end
            end
    end.

parse_integer_obj(Len, ?REDIS_RDB_ENC_INT8, <<Char, Rest/binary>>) ->
    {ok, Char, Len-1, Rest};

parse_integer_obj(_Len, ?REDIS_RDB_ENC_INT8, _) ->
    {error, eof};

parse_integer_obj(Len, ?REDIS_RDB_ENC_INT16, <<A, B, Rest/binary>>) ->
    {ok, A bor (B bsl 8), Len-2, Rest};

parse_integer_obj(_Len, ?REDIS_RDB_ENC_INT16, _) ->
    {error, eof};

parse_integer_obj(Len, ?REDIS_RDB_ENC_INT32, <<A,B,C,D, Rest/binary>>) ->
    {ok, A bor (B bsl 8) bor (C bsl 16) bor (D bsl 24), Len-4, Rest};

parse_integer_obj(_Len, ?REDIS_RDB_ENC_INT32, _) ->
    {error, eof};

parse_integer_obj(_Len, _Type, _Data) ->
    exit("Unknown RDB integer encoding type").

parse_list_vals(Len, 0, Rest, Acc) ->
    {ok, lists:reverse(Acc), Len, Rest};

parse_list_vals(Len, Size, Rest, Acc) ->
    case parse_string(Len, Rest) of
        {ok, Val, Len1, Rest1} ->
            parse_list_vals(Len1, Size-1, Rest1, [Val|Acc]);
        {error, eof} ->
            {error, eof}
    end.

parse_zset_vals(Len, 0, Rest, Acc) ->
    {ok, lists:sort(Acc), Len, Rest};

parse_zset_vals(Len, Size, Rest, Acc) ->
    case parse_string(Len, Rest) of 
        {ok, Val, Len1, Rest1} ->
            case parse_string(Len1, Rest1) of
                {ok, Score, Len2, Rest2} ->
                    parse_zset_vals(Len2, Size-1, Rest2, [{Score, Val}|Acc]);
                {error, eof} ->
                    {error, eof}
            end;
        {error, eof} ->
            {error, eof}
    end.

parse_hash_props(Len, 0, Rest, Acc) ->
    {ok, lists:reverse(Acc), Len, Rest};

parse_hash_props(Len, Size, Rest, Acc) ->
    case parse_string(Len, Rest) of
        {ok, Key, Len1, Rest1} ->
            case parse_string(Len1, Rest1) of
                {ok, Val, Len2, Rest2} ->
                    parse_hash_props(Len2, Size-1, Rest2, [{Key, Val}|Acc]);
                {error, eof} ->
                    {error, eof}
            end;
        {error, eof} ->
            {error, eof}
    end.

parse_commands(<<>>, _Tid, _Map) ->
    {ok, <<>>};

parse_commands(Data, Tid, Map) ->
    parse_commands(Data, Tid, Map, Data).

parse_commands(<<"*", Rest/binary>>, Tid, Map, Orig) ->
    case parse_num(Rest, <<>>) of
        {ok, Num, Rest1} ->
            case parse_num_commands(Rest1, Num, []) of 
                {ok, [Cmd|Args], Rest2} ->
                    dispatch_cmd(Cmd, Args, Tid, Map),
                    parse_commands(Rest2, Tid, Map);
                {error, eof} ->
                    {ok, Orig}
            end;
        {error, eof} ->
            {ok, Orig}
    end.

dispatch_cmd(Cmd, Args, Tid, Map) ->
    Cmd1 = string:to_lower(binary_to_list(Cmd)),
    case dict:find(Cmd1, Map) of
        {ok, Mod} ->
            io:format("dispatch ~p ~p~n", [Cmd1, Args]),
            Mod:handle(Cmd1, Args, Tid);
        error ->
            io:format("unhandled command ~p, ~p~n", [Cmd1, Args])
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
