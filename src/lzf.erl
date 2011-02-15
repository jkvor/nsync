-module(lzf).
-export([compress/1, decompress/1]).

-on_load(init/0).

init() ->
    ok = erlang:load_nif("./priv/esync_drv", 0).

compress(_X) ->
    exit(nif_library_not_loaded).

decompress(_X) ->
    exit(nif_library_not_loaded).

