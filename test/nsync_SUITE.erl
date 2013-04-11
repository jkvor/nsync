-module(nsync_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

%% taken from nysnc.erl
-record(state, {callback, caller_pid, socket, opts, 
                state, buffer, rdb_state}).

all() -> [heartbeat_up, heartbeat_loading].

heartbeat_up(_Config) ->
    Self = self(),
    {ok,Port} = gen_tcp:listen(0, []),
    Pid = proc_lib:spawn(fun() ->
        gen_server:enter_loop(
            nsync,
            [],
            #state{callback=fun(Event) -> Self ! Event end,
                   socket=Port, buffer= <<>>, state=up,
                   opts=[{port,badarg}]}
        )
    end),
    Ref = erlang:monitor(process, Pid),
    Pid ! timeout,
    #state{state=heartbeat} = get_state(sys:get_status(Pid)),
    Pid ! {tcp, Port, <<"+PONG\r\n">>},
    #state{state=up} = get_state(sys:get_status(Pid)),
    Pid ! timeout,
    #state{state=heartbeat} = get_state(sys:get_status(Pid)),
    Pid ! timeout,
    %% We passed in a bad port to see a crash when trying to reconnect.
    %% the port 'badarg' sent in the options should cause an invalid
    %% port error. If we catch it, it works as planned.
    receive
        {'DOWN', Ref, process, Pid, {error,invalid_host_or_port}} -> ok
        after 5000 ->
            exit(Pid, shutdown),
            exit(no_heartbeat)
    end.

heartbeat_loading(_Config) ->
    Self = self(),
    {ok,Port} = gen_tcp:listen(0, []),
    Pid = proc_lib:spawn(fun() ->
        gen_server:enter_loop(
            nsync,
            [],
            #state{callback=fun(Event) -> Self ! Event end,
                   socket=Port, buffer= <<>>, state=loading,
                   opts=[]}
        )
    end),
    Ref = erlang:monitor(process, Pid),
    %% always die in a sync timeout
    Pid ! timeout,
    receive
        {'DOWN', Ref, process, Pid, rdb_load_timeout} -> ok
        after 5000 ->
            exit(Pid, shutdown),
            exit(no_heartbeat)
    end.

%%%%%%%%%%%%%%%
%%% HELPERS %%%
%%%%%%%%%%%%%%%
get_state({status,_Pid,_Mod,List}) ->
    Datas = [proplists:get_all_values(data,L)
        || L = [_|_] <- List, proplists:get_value(data,L) =/= undefined],
    [State] = [State || {"State",State} <- lists:flatten(Datas)],
    State.

