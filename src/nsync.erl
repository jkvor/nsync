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
-export([start_link/0, start_link/1]).

-include("nsync.hrl").

-record(state, {callback, caller_pid, socket, opts, 
                state, buffer, rdb_state, map}).

-define(TIMEOUT, 30000).

%%====================================================================
%% API functions
%%====================================================================
start_link() ->
    start_link([]).

start_link(Opts) ->
    Timeout = proplists:get_value(timeout, Opts, 1000 * 60 * 6),
    case proplists:get_value(block, Opts) of
        true ->
            case gen_server:start_link(?MODULE, [Opts, self()], []) of
                {ok, Pid} ->
                    receive
                        {Pid, load_complete} ->
                            {ok, Pid}
                    after Timeout ->
                        {error, timeout}
                    end;
                Err ->
                    Err
            end;
        _ ->
            gen_server:start_link(?MODULE, [Opts, undefined], [])
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Opts, CallerPid]) ->
    case init_state(Opts, CallerPid, false) of
        {ok, State} ->
            {ok, State};
        Error ->
            {stop, Error}
    end.

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, #state{callback=Callback,
                                        caller_pid=CallerPid,
                                        socket=Socket,
                                        map=Map,
                                        state=loading,
                                        rdb_state=RdbState}=State) ->
    NewState =
        case rdb_load:packet(RdbState, Data, Callback) of
            {eof, Rest} ->
                case CallerPid of
                    undefined -> ok;
                    _ -> CallerPid ! {self(), load_complete}
                end,
                nsync_utils:do_callback(Callback, [{load, eof}]),
                {ok, Rest1} = redis_text_proto:parse_commands(Rest, Callback, Map),
                State#state{state=up, buffer=Rest1};
            RdbState1 ->
                State#state{rdb_state=RdbState1}
        end,
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState};

handle_info({tcp, Socket, Data}, #state{callback=Callback,
                                        socket=Socket,
                                        buffer=Buffer,
                                        map=Map}=State) ->
    {ok, Rest} = redis_text_proto:parse_commands(<<Buffer/binary, Data/binary>>, Callback, Map),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{buffer=Rest}};

handle_info({tcp_closed, _}, #state{callback=Callback,
                                    opts=Opts,
                                    socket=Socket}=State) ->
    catch gen_tcp:close(Socket),
    nsync_utils:do_callback(Callback, [{error, closed}]),
    case init_state(Opts, undefined, true) of
        {ok, State1} ->
            {noreply, State1};
        Error ->
            {stop, Error, State}
    end;

handle_info({tcp_error, _ ,_}, #state{callback=Callback,
                                      opts=Opts,
                                      socket=Socket}=State) ->
    catch gen_tcp:close(Socket),
    nsync_utils:do_callback(Callback, [{error, closed}]),
    case init_state(Opts, undefined, true) of
        {ok, State1} ->
            {noreply, State1};
        Error ->
            {stop, Error, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% internal functions
%%====================================================================
init_state(Opts, CallerPid, Reconnect) ->
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 6379),
    Auth = proplists:get_value(auth, Opts),
    Callback =
        case proplists:get_value(callback, Opts) of
            undefined -> default_callback(Reconnect);
            Cb -> Cb
        end,
    case open_socket(Host, Port) of
        {ok, Socket} ->
            case authenticate(Socket, Auth) of
                ok ->
                    Map = init_map(),
                    init_sync(Socket),
                    inet:setopts(Socket, [{active, once}]),
                    {ok, #state{
                        callback=Callback,
                        caller_pid=CallerPid,
                        socket=Socket,
                        opts=Opts,
                        state=loading,
                        buffer = <<>>,
                        map=Map
                    }};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

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
                {ok, <<"+OK\r\n">>} ->
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

default_callback(Reconnect) ->
    case ets:info(?MODULE, protection) of
        undefined ->
            ets:new(?MODULE, [protected, named_table, set]);
        _ when Reconnect ->
            ets:delete_all_objects(?MODULE);
        _ when not Reconnect ->
            exit("Default nsync ets table already defined")
    end,
    fun({load, _K, _V}) ->
          ?MODULE;
       ({load, eof}) ->
          ok;
       ({error, Error}) ->
          error_logger:error_report([?MODULE, {error, Error}]);
       ({cmd, _Cmd, _Args}) ->
          ?MODULE;
       (_) ->
          undefined
    end.

init_map() ->
    Mods = [nsync_string, nsync_list, nsync_set, nsync_zset, nsync_hash],
    lists:foldl(fun(Mod, Acc) ->
        lists:foldl(fun(Cmd, Acc1) ->
            dict:store(Cmd, Mod, Acc1)
        end, Acc, Mod:command_hooks())
    end, dict:new(), Mods).

init_sync(Socket) ->
    gen_tcp:send(Socket, <<"SYNC\r\n">>).

