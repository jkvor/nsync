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
