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
                state, buffer, rdb_state}).

-define(TIMEOUT, 30000).
%% By default, redis should send us a PING message roughly every second
%% even with data flowing through. If this fails to happen and we receive
%% no data whatsoever in over a minute, we can assume something went wrong.
-define(HEARTBEAT, 60000). % 1 minute

-define(CALLBACK_MODS, [nsync_string,
                        nsync_list,
                        nsync_set,
                        nsync_zset,
                        nsync_hash,
                        nsync_pubsub]).

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
    case init_state(Opts, CallerPid) of
        {ok, State = #state{}} ->
            {ok, State, ?HEARTBEAT};
        Error ->
            {stop, Error}
    end.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ignore, State, ?HEARTBEAT}.

handle_cast(_Msg, State = #state{}) ->
    {noreply, State, ?HEARTBEAT}.

handle_info({tcp, Socket, Data}, #state{callback=Callback,
                                        caller_pid=CallerPid,
                                        socket=Socket,
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
                {ok, Rest1} = redis_text_proto:parse_commands(Rest, Callback),
                State#state{state=up, buffer=Rest1};
            {error, <<"-LOADING", _/binary>> = Msg} ->
                error_logger:info_report([?MODULE, Msg]), 
                timer:sleep(1000),
                init_sync(Socket),
                State;
            RdbState1 ->
                State#state{rdb_state=RdbState1}
        end,
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState, ?HEARTBEAT};

%% We received the heartbeat, keep going!
handle_info({tcp, Socket, <<"+PONG\r\n">>}, #state{socket=Socket,
                                                   state=heartbeat}=State) ->
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{state=up}, ?HEARTBEAT};

handle_info({tcp, Socket, Data}, #state{callback=Callback,
                                        socket=Socket,
                                        buffer=Buffer}=State) ->
    {ok, Rest} = redis_text_proto:parse_commands(<<Buffer/binary, Data/binary>>, Callback),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{buffer=Rest}, ?HEARTBEAT};

handle_info({tcp_closed, _}, #state{callback=Callback,
                                    opts=Opts,
                                    socket=Socket}=State) ->
    catch gen_tcp:close(Socket),
    nsync_utils:do_callback(Callback, [{error, closed}]),
    case reconnect(Opts) of
        {ok, State1} ->
            {noreply, State1, ?HEARTBEAT};
        Error ->
            {stop, Error, State}
    end;

handle_info({tcp_error, _ ,_}, #state{callback=Callback,
                                      opts=Opts,
                                      socket=Socket}=State) ->
    catch gen_tcp:close(Socket),
    nsync_utils:do_callback(Callback, [{error, closed}]),
    case reconnect(Opts) of    
        {ok, State1} ->
            {noreply, State1, ?HEARTBEAT};
        Error ->
            {stop, Error, State}
    end;

%% Heartbeat timer is off and we were loading data. Not normal.
handle_info(timeout, State = #state{state=loading}) ->
    {stop, rdb_load_timeout, State};

%% Heartbeat timer is off while streaming data. The connection probably
%% has something wrong with it. We send an inline PING command, which should
%% have the server sending us a +PONG response back in a packet.
handle_info(timeout, State = #state{state=up, socket=Socket}) ->
    gen_tcp:send(Socket, <<"PING\r\n">>),
    {noreply, State#state{state=heartbeat}, ?HEARTBEAT};

%% The heartbeat timed out. Assume the connection is broken.
handle_info(timeout, State = #state{state=heartbeat, socket=Socket,
                                    callback=Callback, opts=Opts}) ->
    catch gen_tcp:close(Socket),
    nsync_utils:do_callback(Callback, [{error, closed}]),
    case reconnect(Opts) of
        {ok, State1} ->
            {noreply, State1, ?HEARTBEAT};
        Error ->
            ct:pal("Error: ~p",[Error]),
            {stop, Error, State}
    end;

handle_info(_Info, State = #state{}) ->
    {noreply, State, ?HEARTBEAT}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State, ?HEARTBEAT}.

%%====================================================================
%% internal functions
%%====================================================================
reconnect(Opts) ->
    case init_state(Opts, undefined) of
        {ok, State} ->
            {ok, State};
        {error, Err} when Err == econnrefused; Err == closed ->
            timer:sleep(250),
            reconnect(Opts);
        Err ->
            Err
    end.

init_state(Opts, CallerPid) ->
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 6379),
    Auth = proplists:get_value(pass, Opts),
    Callback =
        case proplists:get_value(callback, Opts) of
            undefined -> default_callback();
            Cb -> Cb
        end,
    case open_socket(Host, Port) of
        {ok, Socket} ->
            case authenticate(Socket, Auth) of
                ok ->
                    init_sync(Socket),
                    inet:setopts(Socket, [{active, once}]),
                    {ok, #state{
                        callback=Callback,
                        caller_pid=CallerPid,
                        socket=Socket,
                        opts=Opts,
                        state=loading,
                        buffer = <<>>
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
                {ok, Other} ->
                    {error, {ok, Other}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

default_callback() ->
    fun(Arg) ->
          io:format("nsync ~1000p~n", [Arg])
    end.

init_sync(Socket) ->
    gen_tcp:send(Socket, <<"SYNC\r\n">>).

