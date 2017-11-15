package com.feeyo.redis.net.front.handler;

import com.feeyo.redis.engine.codec.RedisRequestPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author zhuam
 *
 */
public class CommandParse {
	
	// 特殊指令策略
	public static final int UNKNOW_CMD   	= -1;		// 未知指令
	public static final int DISABLED_CMD 	= 0;		// 禁止指令
	public static final int CLUSTER_CMD 	= 1;		// 集群指令
	public static final int NO_CLUSTER_CMD 	= 2;		// 非集群指令
	public static final int MANAGE_CMD 		= 3;		// 管理指令
	public static final int PUBSUB_CMD 		= 4;		// 
	public static final int THROUGH_CMD 	= 5;		// 透传指令

	public static final int AUTO_RESP_CMD	= 7;		// 中间件应答指令
	public static final int MGETSET_CMD		= 9;		// 中间件加强指令
	public static final int DEL_CMD			= 10;		// 中间件加强指令
	
	// RW 指令
	public static final byte WRITE_CMD = 1;
	public static final byte READ_CMD = 2;
	public static final byte DELETE_CMD = 3;
	
	// type 类型
	public static final byte TYPE_MANAGE_CMD = 1;
	public static final byte TYPE_KEY_CMD = 2;
	public static final byte TYPE_STRING_CMD = 3;
	public static final byte TYPE_HASH_CMD = 4;
	public static final byte TYPE_LIST_CMD = 5;
	public static final byte TYPE_SET_CMD = 6;
	public static final byte TYPE_SORTEDSET_CMD = 7;
	public static final byte TYPE_PUBSUB_CMD = 8;
	public static final byte TYPE_TRANSACTION_CMD = 9;
	public static final byte TYPE_SCRIPT_CMD = 10;
	public static final byte TYPE_CONNECTION_CMD = 11;
	public static final byte TYPE_SERVER_CMD = 12;
	public static final byte TYPE_CLUSTER_CMD = 13;
	public static final byte TYPE_OTHER_CMD = 14;
	
	

	
	private static final Map<String, RedisRequestPolicy> _cmds = new HashMap<String, RedisRequestPolicy>();
	static {		
		// Manage
		_cmds.put("SHOW", 			new RedisRequestPolicy(MANAGE_CMD, READ_CMD, TYPE_MANAGE_CMD));
		_cmds.put("RELOAD", 		new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, TYPE_MANAGE_CMD));
		_cmds.put("JVM", 			new RedisRequestPolicy(MANAGE_CMD, READ_CMD, TYPE_MANAGE_CMD));
		_cmds.put("ZK", 			new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, TYPE_MANAGE_CMD));
		_cmds.put("NODE", 			new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, TYPE_MANAGE_CMD));
		_cmds.put("USE", 			new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, TYPE_MANAGE_CMD));
		// Cluster
		_cmds.put("CLUSTER", 		new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, TYPE_CLUSTER_CMD));

		//Key
		_cmds.put("DEL", 			new RedisRequestPolicy(DEL_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("DUMP", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_KEY_CMD));		//返回被序列化的值
		_cmds.put("EXISTS", 		new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_KEY_CMD));
		_cmds.put("EXPIRE", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("EXPIREAT", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("KEYS", 			new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, TYPE_KEY_CMD));
		_cmds.put("MIGRATE", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("MOVE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("OBJECT", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("PERSIST", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_KEY_CMD));		//移除给定 key 的生存时间
		_cmds.put("PEXPIRE", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_KEY_CMD));		//它以毫秒为单位设置 key 的生存时间
		_cmds.put("PEXPIREAT", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_KEY_CMD)); 		//以毫秒为单位设置 key 的过期 unix 时间戳
		_cmds.put("PTTL", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_KEY_CMD));	 	//以毫秒为单位返回 key 的剩余生存时间
		_cmds.put("RANDOMKEY", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("RENAME", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("RENAMENX", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("RESTORE", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_KEY_CMD));		//反序列化给定的序列化值，并将它和给定的 key 关联
		_cmds.put("SORT", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_KEY_CMD));
		_cmds.put("TTL", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_KEY_CMD));
		_cmds.put("TYPE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_KEY_CMD));
		_cmds.put("SCAN", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_KEY_CMD));
		
		
		//String
		_cmds.put("APPEND", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("BITCOUNT", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_STRING_CMD));
		_cmds.put("BITOP", 				new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("BITFIELD", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("DECR", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("DECRBY", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("GET", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_STRING_CMD));
		_cmds.put("GETBIT", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_STRING_CMD));
		_cmds.put("GETRANGE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_STRING_CMD));
		_cmds.put("GETSET", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("INCR", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("INCRBY", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("INCRBYFLOAT", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("MGET", 				new RedisRequestPolicy(MGETSET_CMD, READ_CMD, TYPE_STRING_CMD));
		_cmds.put("MSET", 				new RedisRequestPolicy(MGETSET_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("MSETNX", 			new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("PSETEX", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));			//以毫秒为单位设置 key 的生存时间
		_cmds.put("SET", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("SETBIT", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("SETEX", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("SETNX", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("SETRANGE", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_STRING_CMD));
		_cmds.put("STRLEN", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_STRING_CMD));
		
		//Hash
		_cmds.put("HDEL", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_HASH_CMD));
		_cmds.put("HEXISTS", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_HASH_CMD));
		_cmds.put("HGET", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_HASH_CMD));
		_cmds.put("HGETALL", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_HASH_CMD));
		_cmds.put("HINCRBY", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_HASH_CMD));
		_cmds.put("HINCRBYFLOAT", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_HASH_CMD));
		_cmds.put("HKEYS", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_HASH_CMD));
		_cmds.put("HLEN", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_HASH_CMD));
		_cmds.put("HMGET",				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_HASH_CMD));
		_cmds.put("HMSET", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_HASH_CMD));
		_cmds.put("HSET", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_HASH_CMD));
		_cmds.put("HSETNX", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_HASH_CMD));
		_cmds.put("HVALS", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_HASH_CMD));
		_cmds.put("HSCAN", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_HASH_CMD));
		_cmds.put("HSTRLEN", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_HASH_CMD));
		
		// List
		_cmds.put("BLPOP", 				new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, TYPE_LIST_CMD));
		_cmds.put("BRPOP", 				new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, TYPE_LIST_CMD));
		_cmds.put("BRPOPLPUSH", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_LIST_CMD));
		_cmds.put("LINDEX", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_LIST_CMD));
		_cmds.put("LINSERT", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_LIST_CMD));
		_cmds.put("LLEN", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_LIST_CMD));
		_cmds.put("LPOP", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_LIST_CMD));
		_cmds.put("LPUSH", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_LIST_CMD));
		_cmds.put("LPUSHX", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_LIST_CMD));
		_cmds.put("LRANGE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_LIST_CMD));
		_cmds.put("LREM", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_LIST_CMD));
		_cmds.put("LSET", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_LIST_CMD));
		_cmds.put("LTRIM", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_LIST_CMD));
		_cmds.put("RPOP", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_LIST_CMD));
		_cmds.put("RPOPLPUSH", 			new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_LIST_CMD));
		_cmds.put("RPUSH", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_LIST_CMD));
		_cmds.put("RPUSHX", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_LIST_CMD));
		
		// Set
		_cmds.put("SADD", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SET_CMD));
		_cmds.put("SCARD", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SET_CMD));
		_cmds.put("SISMEMBER", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SET_CMD));
		_cmds.put("SMEMBERS", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SET_CMD));
		_cmds.put("SMOVE", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SET_CMD));
		_cmds.put("SPOP", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SET_CMD));
		_cmds.put("SRANDMEMBER", 		new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SET_CMD));
		_cmds.put("SREM", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SET_CMD));
		_cmds.put("SSCAN", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SET_CMD));
		
		_cmds.put("SDIFF", 				new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, TYPE_SET_CMD));
		_cmds.put("SDIFFSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_SET_CMD));
		_cmds.put("SINTER", 			new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, TYPE_SET_CMD));
		_cmds.put("SINTERSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_SET_CMD));
		_cmds.put("SUNION", 			new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, TYPE_SET_CMD));
		_cmds.put("SUNIONSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_SET_CMD));
		
		
		// SortedSet
		_cmds.put("ZADD", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZCARD", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZCOUNT", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZINCRBY", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZRANGE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZRANGEBYSCORE",  	new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZRANK", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZREM", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZREMRANGEBYRANK",	new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZREMRANGEBYSCORE", 	new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZREVRANGE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZREVRANGEBYSCORE", 	new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZREVRANK", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZSCORE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZUNIONSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZINTERSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZSCAN", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZRANGEBYLEX", 		new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZLEXCOUNT", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SORTEDSET_CMD));
		_cmds.put("ZREMRANGEBYLEX", 	new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_SORTEDSET_CMD));
		
		// HyperLogLog
		_cmds.put("PFADD", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_OTHER_CMD));
		_cmds.put("PFCOUNT", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_OTHER_CMD));
		_cmds.put("PFMERGE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_OTHER_CMD));
		
		// Geo
		_cmds.put("GEOADD", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_OTHER_CMD));
		_cmds.put("GEOPOS", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_OTHER_CMD));
		_cmds.put("GEODIST", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_OTHER_CMD));
		_cmds.put("GEORADIUS", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_OTHER_CMD));
		_cmds.put("GEORADIUSBYMEMBER", 	new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_OTHER_CMD));
		_cmds.put("GEOHASH", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_OTHER_CMD));
		
		// Pub/Sub
		_cmds.put("PUBSUB", 			new RedisRequestPolicy(DISABLED_CMD, READ_CMD, TYPE_PUBSUB_CMD));		// 不支持
		_cmds.put("PUBLISH", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, TYPE_PUBSUB_CMD));
		_cmds.put("PSUBSCRIBE", 		new RedisRequestPolicy(PUBSUB_CMD, READ_CMD, TYPE_PUBSUB_CMD));
		_cmds.put("PUNSUBSCRIBE", 		new RedisRequestPolicy(PUBSUB_CMD, READ_CMD, TYPE_PUBSUB_CMD));
		_cmds.put("SUBSCRIBE", 			new RedisRequestPolicy(PUBSUB_CMD, READ_CMD, TYPE_PUBSUB_CMD));
		_cmds.put("UNSUBSCRIBE", 		new RedisRequestPolicy(PUBSUB_CMD, READ_CMD, TYPE_PUBSUB_CMD));
		
		// Transaction
		_cmds.put("MULTI", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_TRANSACTION_CMD));	//开启事务
		_cmds.put("EXEC", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_TRANSACTION_CMD)); //提交事务
		_cmds.put("DISCARD", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_TRANSACTION_CMD));	//取消事务
		_cmds.put("WATCH", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_TRANSACTION_CMD));	//监视
		_cmds.put("UNWATCH", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_TRANSACTION_CMD)); //取消监视
		
		// Script 
		_cmds.put("EVAL", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SCRIPT_CMD));
		_cmds.put("EVALSHA", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SCRIPT_CMD));
		_cmds.put("SCRIPT", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SCRIPT_CMD));
		
		// Connection
		_cmds.put("AUTH", 				new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, TYPE_CONNECTION_CMD));
		_cmds.put("ECHO", 				new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, TYPE_CONNECTION_CMD));
		_cmds.put("PING", 				new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, TYPE_CONNECTION_CMD));
		_cmds.put("QUIT", 				new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, TYPE_CONNECTION_CMD));
		_cmds.put("SELECT", 			new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, TYPE_CONNECTION_CMD));
		
		// Server 
		_cmds.put("BGREWRITEAOF", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("BGSAVE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("GETNAME", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));

		_cmds.put("CLIENT", 			new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		
		_cmds.put("SETNAME", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("CONFIG", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("RESETSTAT", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("REWRITE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("DBSIZE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("DEBUG", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("SEGFAULT", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("FLUSHALL", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("FLUSHDB", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		
		_cmds.put("INFO", 				new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, TYPE_SERVER_CMD));
		
		_cmds.put("LASTSAVE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("MONITOR", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("PSYNC", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("SAVE", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("SHUTDOWN", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("SLAVEOF", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("SLOWLOG", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("SYNC", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("TIME", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, TYPE_SERVER_CMD));
		
		_cmds.put("COMMAND", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));		// command count/getkeys/info/
		_cmds.put("WAIT", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));
		_cmds.put("ROLE", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));		
		_cmds.put("READONLY", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));		// 执行该命令后，可以在slave上执行只读命令
		_cmds.put("READWRITE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));		// 执行该命令后，取消在slave上执行命令
		_cmds.put("TOUCH", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));		// 
		_cmds.put("UNLINK", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, TYPE_SERVER_CMD));		// 

	}
    
    // 解析特殊指令策略
	public static RedisRequestPolicy getPolicy(String cmd) {
		RedisRequestPolicy policy = _cmds.get( cmd );
		return policy == null ? new RedisRequestPolicy(UNKNOW_CMD, (byte)-1, (byte)-1) : policy;		
	}
	
	
}
