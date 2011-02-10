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

-record(state, {tid, socket, opts, state, len_remaining, buffer}).

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
                    init_table(Tid),
                    init_sync(Socket),
                    inet:setopts(Socket, [{active, once}]),
                    {ok, #state{tid=Tid, socket=Socket, opts=Opts, state=loading, buffer = <<>>}};
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
                                        len_remaining=Len}=State) ->
    {ok, Len1, Rest1, NewState} = parse_keys(Len, Data, Tid),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{len_remaining=Len1, state=NewState, buffer=Rest1}};

handle_info({tcp, Socket, Data}, #state{socket=Socket}=State) ->
    io:format("~p~n", [Data]),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State};

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
    {ok, _Db, Len1, Rest1} = rdb_len(Len-1, Rest, undefined),
    parse_keys(Len1, Rest1, Tid);

parse_keys(Len, <<?REDIS_STRING, Rest/binary>>, Tid) ->
    {ok, Key, Len1, Rest1} = parse_string(Len-1, Rest),
    {ok, Val, Len2, Rest2} = parse_string(Len1, Rest1),
    ets:insert(Tid, {Key, Val}),
    parse_keys(Len2, Rest2, Tid);    

parse_keys(Len, <<?REDIS_LIST, Rest/binary>>, Tid) ->
    {ok, Key, Len1, Rest1} = parse_string(Len-1, Rest),
    {ok, Size, Len2, Rest2} = rdb_len(Len1, Rest1, undefined),
    {ok, Vals, Len3, Rest3} = parse_list_vals(Len2, Size, Rest2, []),
    ets:insert(Tid, {Key, Vals}),
    parse_keys(Len3, Rest3, Tid);

parse_keys(Len, <<?REDIS_SET, Rest/binary>>, Tid) ->
    {ok, Key, Len1, Rest1} = parse_string(Len-1, Rest),
    {ok, Size, Len2, Rest2} = rdb_len(Len1, Rest1, undefined),
    {ok, Vals, Len3, Rest3} = parse_list_vals(Len2, Size, Rest2, []),
    ets:insert(Tid, {Key, Vals}),
    parse_keys(Len3, Rest3, Tid);

parse_keys(Len, <<?REDIS_ZSET, Rest/binary>>, Tid) ->
    {ok, Key, Len1, Rest1} = parse_string(Len-1, Rest),
    {ok, Size, Len2, Rest2} = rdb_len(Len1, Rest1, undefined),
    {ok, Vals, Len3, Rest3} = parse_zset_vals(Len2, Size, Rest2, []),
    ets:insert(Tid, {Key, Vals}),
    parse_keys(Len3, Rest3, Tid);    

parse_keys(Len, <<?REDIS_HASH, Rest/binary>>, Tid) ->
    {ok, Key, Len1, Rest1} = parse_string(Len-1, Rest),
    {ok, Size, Len2, Rest2} = rdb_len(Len1, Rest1, undefined),
    {ok, Props, Len3, Rest3} = parse_hash_props(Len2, Size, Rest2, []),
    ets:insert(Tid, {Key, Props}),
    parse_keys(Len3, Rest3, Tid).

parse_string(Len, Data) ->
    {ok, Size, Len1, Rest} = rdb_len(Len, Data, undefined),
    <<Str:Size/binary, Rest1/binary>> = Rest,
    {ok, Str, Len1-Size, Rest1}.

parse_list_vals(Len, 0, Rest, Acc) ->
    {ok, lists:reverse(Acc), Len, Rest};

parse_list_vals(Len, Size, Rest, Acc) ->
    {ok, Val, Len1, Rest1} = parse_string(Len, Rest),
    parse_list_vals(Len1, Size-1, Rest1, [Val|Acc]).

parse_zset_vals(Len, 0, Rest, Acc) ->
    {ok, lists:sort(Acc), Len, Rest};

parse_zset_vals(Len, Size, Rest, Acc) ->
    {ok, Val, Len1, Rest1} = parse_string(Len, Rest),
    {ok, Score, Len2, Rest2} = parse_string(Len1, Rest1),
    parse_zset_vals(Len2, Size-1, Rest2, [{Score, Val}|Acc]).

parse_hash_props(Len, 0, Rest, Acc) ->
    {ok, lists:reverse(Acc), Len, Rest};

parse_hash_props(Len, Size, Rest, Acc) ->
    {ok, Key, Len1, Rest1} = parse_string(Len, Rest),
    {ok, Val, Len2, Rest2} = parse_string(Len1, Rest1),
    parse_hash_props(Len2, Size-1, Rest2, [{Key, Val}|Acc]).

rdb_len(Len, <<Type, Rest/binary>>, _IsEncoded) ->
    case ((Type band 16#C0) bsr 6) of
        ?REDIS_RDB_6BITLEN ->
            {ok, Type band 16#3F, Len-1, Rest};
        ?REDIS_RDB_ENCVAL ->
            {ok, Type band 16#3F, Len-1, Rest};
        ?REDIS_RDB_14BITLEN ->
            <<Next, Rest1/binary>> = Rest,
            {ok, ((Type band 16#3F) bsl 8) bor Next, Len-2, Rest1};
        _ ->
            <<Next:3, Rest1/binary>> = Rest,
            <<Val:4/unsigned-integer>> = <<Type, Next/binary>>,
            {ok, Val, Len-4, Rest1}
    end.
