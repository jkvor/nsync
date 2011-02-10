-define(REDIS_STRING, 0).
-define(REDIS_LIST, 1).
-define(REDIS_SET, 2).
-define(REDIS_ZSET, 3).
-define(REDIS_HASH, 4).
-define(REDIS_VMPOINTER, 8).

-define(REDIS_EXPIRETIME, 253).
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

