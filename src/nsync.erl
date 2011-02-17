-module(nsync).
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

-include("nsync.hrl").

-record(state, {tid, socket, opts, state, buffer, rdb_state, map}).

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
                                        rdb_state=RdbState}=State) ->
    NewState =
        case rdb_load:packet(RdbState, Data, Tid) of
            {error, eof} ->
                error_logger:info_msg("rdb_load complete~n"),
                State#state{state=up};
            RdbState1 ->
                State#state{rdb_state=RdbState1}
        end,
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState};

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
    Mods = [nsync_string, nsync_list, nsync_set, nsync_zset, nsync_hash],
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
