-module(nsync_utils).
-compile(export_all).

do_callback({M,F}, Args) when is_atom(M), is_atom(F) ->
    apply(M, F, Args);

do_callback({M,F,A}, Args) when is_atom(M), is_atom(F), is_list(A) ->
    apply(M, F, A ++ Args);

do_callback(Fun, Args) when is_function(Fun) ->
    apply(Fun, Args);

do_callback(_Cb, _Args) ->
    exit("Invalid callback").

lookup_write_tid(MasterTid, Name) ->
    case ets:lookup(MasterTid, Name) of
        [{Name, _R, W}] ->
            W;
        [] ->
            W = ets:new(undefined, [protected, set]),
            ets:insert(MasterTid, {Name, W, W}),
            W
    end.

lookup_read_tid(MasterTid, Name) ->
    case ets:lookup(MasterTid, Name) of
        [{Name, R, _W}] ->
            R;
        [] ->
            undefined
    end.

reset_write_tids(MasterTid) ->
    [begin
        W1 = ets:new(undefined, [protected, set]), 
        ets:insert(MasterTid, {Name, R, W1})
    end || {Name, R, _W} <- ets:tab2list(MasterTid)].

failover_tids(MasterTid) ->
    [begin
        ets:insert(MasterTid, {Name, W, W}),
        R =/= W andalso ets:delete(R)
    end || {Name, R, W} <- ets:tab2list(MasterTid)].
