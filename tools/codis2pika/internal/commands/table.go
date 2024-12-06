package commands

var containers = map[string]bool{
	"XGROUP":   true,
	"FUNCTION": true,
}
var redisCommands = map[string]redisCommand{
	"MSETNX": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				-1,
				2,
				0,
				0,
				0,
				0,
			},
		},
	},
	"GETEX": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"INCRBYFLOAT": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"MSET": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				-1,
				2,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SET": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"DECRBY": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"INCRBY": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SETEX": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"DECR": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"INCR": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"PSETEX": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"GETSET": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SETRANGE": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"APPEND": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SETNX": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"GETDEL": {
		"STRING",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SDIFFSTORE": {
		"SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				-1,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SINTERSTORE": {
		"SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				-1,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SUNIONSTORE": {
		"SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				-1,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SPOP": {
		"SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SADD": {
		"SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SREM": {
		"SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SMOVE": {
		"SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"GEORADIUS": {
		"GEO",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"keyword",
				0,
				"STORE",
				6,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"keyword",
				0,
				"STOREDIST",
				6,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"GEOADD": {
		"GEO",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"GEOSEARCHSTORE": {
		"GEO",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"GEORADIUSBYMEMBER": {
		"GEO",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"keyword",
				0,
				"STORE",
				5,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"keyword",
				0,
				"STOREDIST",
				5,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"ZPOPMAX": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"ZREMRANGEBYSCORE": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"ZRANGESTORE": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"ZINTERSTORE": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"keynum",
				0,
				0,
				0,
				0,
				1,
				1,
			},
		},
	},
	"ZPOPMIN": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"ZINCRBY": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"ZDIFFSTORE": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"keynum",
				0,
				0,
				0,
				0,
				1,
				1,
			},
		},
	},
	"ZUNIONSTORE": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"keynum",
				0,
				0,
				0,
				0,
				1,
				1,
			},
		},
	},
	"ZREMRANGEBYLEX": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"ZMPOP": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"keynum",
				0,
				0,
				0,
				0,
				1,
				1,
			},
		},
	},
	"ZADD": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"ZREMRANGEBYRANK": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"ZREM": {
		"SORTED_SET",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"LMPOP": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"keynum",
				0,
				0,
				0,
				0,
				1,
				1,
			},
		},
	},
	"LSET": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"RPOPLPUSH": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"LTRIM": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"LPUSH": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"LINSERT": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"LREM": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"RPUSH": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"RPOP": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"LPOP": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"LMOVE": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"RPUSHX": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"LPUSHX": {
		"LIST",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"FUNCTION-FLUSH": {
		"SCRIPTING",
		[]keySpec{},
	},
	"FUNCTION-DELETE": {
		"SCRIPTING",
		[]keySpec{},
	},
	"FUNCTION-RESTORE": {
		"SCRIPTING",
		[]keySpec{},
	},
	"FUNCTION-LOAD": {
		"SCRIPTING",
		[]keySpec{},
	},
	"EVALSHA": {
		"SCRIPTING",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"keynum",
				0,
				0,
				0,
				0,
				1,
				1,
			},
		},
	},
	"FCALL": {
		"SCRIPTING",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"keynum",
				0,
				0,
				0,
				0,
				1,
				1,
			},
		},
	},
	"EVAL": {
		"SCRIPTING",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"keynum",
				0,
				0,
				0,
				0,
				1,
				1,
			},
		},
	},
	"XCLAIM": {
		"STREAM",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XGROUP-DELCONSUMER": {
		"STREAM",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XACK": {
		"STREAM",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XTRIM": {
		"STREAM",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XGROUP-CREATE": {
		"STREAM",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XDEL": {
		"STREAM",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XAUTOCLAIM": {
		"STREAM",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XGROUP-DESTROY": {
		"STREAM",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XADD": {
		"STREAM",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XSETID": {
		"STREAM",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XGROUP-SETID": {
		"STREAM",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"XGROUP-CREATECONSUMER": {
		"STREAM",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"RESTORE": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"UNLINK": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				-1,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"MOVE": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"COPY": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"PERSIST": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"DEL": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				-1,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"PEXPIREAT": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"RENAME": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"RENAMENX": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"PEXPIRE": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"EXPIRE": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"EXPIREAT": {
		"GENERIC",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"PFCOUNT": {
		"HYPERLOGLOG",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				-1,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"PFMERGE": {
		"HYPERLOGLOG",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				2,
				"",
				0,
				"range",
				-1,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"PFADD": {
		"HYPERLOGLOG",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"PFDEBUG": {
		"HYPERLOGLOG",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"FLUSHDB": {
		"SERVER",
		[]keySpec{},
	},
	"SWAPDB": {
		"SERVER",
		[]keySpec{},
	},
	"FLUSHALL": {
		"SERVER",
		[]keySpec{},
	},
	"RESTORE-ASKING": {
		"SERVER",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SETBIT": {
		"BITMAP",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"BITOP": {
		"BITMAP",
		[]keySpec{
			{
				"index",
				2,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
			{
				"index",
				3,
				"",
				0,
				"range",
				-1,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"BITFIELD": {
		"BITMAP",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"HMSET": {
		"HASH",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"HINCRBYFLOAT": {
		"HASH",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"HDEL": {
		"HASH",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"HSETNX": {
		"HASH",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"HSET": {
		"HASH",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"HINCRBY": {
		"HASH",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"SPUBLISH": {
		"PUBSUB",
		[]keySpec{
			{
				"index",
				1,
				"",
				0,
				"range",
				0,
				1,
				0,
				0,
				0,
				0,
			},
		},
	},
	"PUBLISH": {
		"PUBSUB",
		[]keySpec{},
	},
	"PING": {
		"CONNECTION",
		[]keySpec{},
	},
	"SELECT": {
		"CONNECTION",
		[]keySpec{},
	},
}
