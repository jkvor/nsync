-define(REDIS_STRING, 0).
-define(REDIS_LIST, 1).
-define(REDIS_SET, 2).
-define(REDIS_ZSET, 3).
-define(REDIS_HASH, 4).

-define(REDIS_ZMAP, 9).
-define(REDIS_ZLIST, 10).
-define(REDIS_INTSET, 11).
-define(REDIS_SSZLIST, 12).
-define(REDIS_HMAPZLIST, 13).

-define(REDIS_EXPIRETIME_MS, 252).
-define(REDIS_EXPIRETIME_SEC, 253).
-define(REDIS_SELECTDB, 254).
-define(REDIS_EOF, 255).

-define(REDIS_RDB_6BITLEN, 0).
-define(REDIS_RDB_14BITLEN, 1).
-define(REDIS_RDB_32BITLEN, 2).
-define(REDIS_RDB_ENCVAL, 3).

-define(REDIS_RDB_ENC_INT8, 0). % 8 bit signed integer
-define(REDIS_RDB_ENC_INT16, 1). % 16 bit signed integer
-define(REDIS_RDB_ENC_INT32, 2). % 32 bit signed integer
-define(REDIS_RDB_ENC_LZF, 3). % string compressed with FASTLZ


