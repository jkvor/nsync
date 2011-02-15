#include "erl_nif.h"
#include "erl_nif_compat.h"
#include "lzf.h"
#include <errno.h>

extern int decompress(int x);

int decompress1(ErlNifEnv *env, ErlNifBinary *source, ErlNifBinary *target) {
    int retval = 0;
    int bufsize;
    double expansion_factor = 1.1;
    unsigned int result;
    while(expansion_factor < 2.5) {
        bufsize = (int) source->size * expansion_factor;
        bufsize = bufsize < 66 ? 66 : bufsize;
        enif_alloc_binary_compat(env, bufsize, target);
        result = lzf_decompress(source->data, source->size, target->data, target->size);
        if (result) {
            enif_realloc_binary_compat(env, target, result);
            retval = 1;
            break;
        }
        else {
            enif_release_binary_compat(env, target);
        }
        expansion_factor += 0.1;
    }
    return retval;
}
     
int compress1(ErlNifEnv *env, ErlNifBinary *source, ErlNifBinary *target) {
    int retval = 0;
    int bufsize;
    double expansion_factor = 1.1;
    int result;
    while(expansion_factor < 2.5) {
        bufsize = (int) source->size * expansion_factor;
        bufsize = bufsize < 66 ? 66 : bufsize; 
        enif_alloc_binary_compat(env, bufsize, target);
        result = lzf_compress(source->data, source->size, target->data, target->size);
        if (result) {
            enif_realloc_binary_compat(env, target, result);
            retval = 1;
            break;
        }
        else {
            enif_release_binary_compat(env, target);
        }
        expansion_factor += 0.1;
    }
    return retval;
}

static ERL_NIF_TERM decompress_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ERL_NIF_TERM retval;
    ErlNifBinary source;
    ErlNifBinary target;
    if (argc != 1 || !enif_inspect_binary(env, argv[0], &source)) {
        return enif_make_badarg(env);
    }
    if (decompress1(env, &source, &target)) {
        retval = enif_make_binary(env, &target);
    }
    else {
        retval = enif_make_badarg(env);
    }
    return retval;
}
    
ERL_NIF_TERM compress_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ERL_NIF_TERM retval;
    ErlNifBinary source;
    ErlNifBinary target;

    if (argc != 1 || !enif_inspect_binary(env, argv[0], &source)) {
        return enif_make_badarg(env);
    }
    if (compress1(env, &source, &target)) {
        retval = enif_make_binary(env, &target);
    }
    else {
        retval = enif_make_badarg(env);
    }
    return retval;
}

static ErlNifFunc nif_funcs[] = {
    {"decompress", 1, decompress_nif},
    {"compress", 1, compress_nif}
};

ERL_NIF_INIT(lzf, nif_funcs, NULL, NULL, NULL, NULL);
