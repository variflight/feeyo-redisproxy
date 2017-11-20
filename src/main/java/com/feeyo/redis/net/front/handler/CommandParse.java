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
	
	// WATCH
	public static final byte NO_WATCH = -1;
	public static final byte HASH_WATCH = 1;
	public static final byte LIST_WATCH = 2;
	public static final byte SET_WATCH = 3;
	public static final byte SORTED_SET_WATCH = 4;
	
	
	

	
	private static final Map<String, RedisRequestPolicy> _cmds = new HashMap<String, RedisRequestPolicy>();
	static {		
		// Manage
		_cmds.put("SHOW", 			new RedisRequestPolicy(MANAGE_CMD, READ_CMD, NO_WATCH));
		_cmds.put("RELOAD", 		new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("JVM", 			new RedisRequestPolicy(MANAGE_CMD, READ_CMD, NO_WATCH));
		_cmds.put("ZK", 			new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("NODE", 			new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("USE", 			new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, NO_WATCH));
		// Cluster
		_cmds.put("CLUSTER", 		new RedisRequestPolicy(MANAGE_CMD, WRITE_CMD, NO_WATCH));

		//Key
		_cmds.put("DEL", 			new RedisRequestPolicy(DEL_CMD, DELETE_CMD, NO_WATCH));
		_cmds.put("DUMP", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));		//返回被序列化的值
		_cmds.put("EXISTS", 		new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("EXPIRE", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("EXPIREAT", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("KEYS", 			new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, NO_WATCH));
		_cmds.put("MIGRATE", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("MOVE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("OBJECT", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("PERSIST", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));		//移除给定 key 的生存时间
		_cmds.put("PEXPIRE", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));		//它以毫秒为单位设置 key 的生存时间
		_cmds.put("PEXPIREAT", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH)); 		//以毫秒为单位设置 key 的过期 unix 时间戳
		_cmds.put("PTTL", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));	 	//以毫秒为单位返回 key 的剩余生存时间
		_cmds.put("RANDOMKEY", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("RENAME", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("RENAMENX", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("RESTORE", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));		//反序列化给定的序列化值，并将它和给定的 key 关联
		_cmds.put("SORT", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("TTL", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("TYPE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("SCAN", 			new RedisRequestPolicy(DISABLED_CMD, READ_CMD, NO_WATCH));
		
		
		//String
		_cmds.put("APPEND", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("BITCOUNT", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("BITOP", 				new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("BITFIELD", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("DECR", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("DECRBY", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("GET", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("GETBIT", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("GETRANGE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("GETSET", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("INCR", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("INCRBY", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("INCRBYFLOAT", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("MGET", 				new RedisRequestPolicy(MGETSET_CMD, READ_CMD, NO_WATCH));
		_cmds.put("MSET", 				new RedisRequestPolicy(MGETSET_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("MSETNX", 			new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("PSETEX", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));			//以毫秒为单位设置 key 的生存时间
		_cmds.put("SET", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SETBIT", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SETEX", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SETNX", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SETRANGE", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("STRLEN", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		
		//Hash
		_cmds.put("HDEL", 				new RedisRequestPolicy(THROUGH_CMD, DELETE_CMD, HASH_WATCH));
		_cmds.put("HEXISTS", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, HASH_WATCH));
		_cmds.put("HGET", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, HASH_WATCH));
		_cmds.put("HGETALL", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, HASH_WATCH));
		_cmds.put("HINCRBY", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, HASH_WATCH));
		_cmds.put("HINCRBYFLOAT", 		new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, HASH_WATCH));
		_cmds.put("HKEYS", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, HASH_WATCH));
		_cmds.put("HLEN", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, HASH_WATCH));
		_cmds.put("HMGET",				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, HASH_WATCH));
		_cmds.put("HMSET", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, HASH_WATCH));
		_cmds.put("HSET", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, HASH_WATCH));
		_cmds.put("HSETNX", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, HASH_WATCH));
		_cmds.put("HVALS", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, HASH_WATCH));
		_cmds.put("HSCAN", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, HASH_WATCH));
		_cmds.put("HSTRLEN", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, HASH_WATCH));
		
		// List
		_cmds.put("BLPOP", 				new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, LIST_WATCH));
		_cmds.put("BRPOP", 				new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, LIST_WATCH));
		_cmds.put("BRPOPLPUSH", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, LIST_WATCH));
		_cmds.put("LINDEX", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, LIST_WATCH));
		_cmds.put("LINSERT", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, LIST_WATCH));
		_cmds.put("LLEN", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, LIST_WATCH));
		_cmds.put("LPOP", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, LIST_WATCH));
		_cmds.put("LPUSH", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, LIST_WATCH));
		_cmds.put("LPUSHX", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, LIST_WATCH));
		_cmds.put("LRANGE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, LIST_WATCH));
		_cmds.put("LREM", 				new RedisRequestPolicy(THROUGH_CMD, DELETE_CMD, LIST_WATCH));
		_cmds.put("LSET", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, LIST_WATCH));
		_cmds.put("LTRIM", 				new RedisRequestPolicy(THROUGH_CMD, DELETE_CMD, LIST_WATCH));
		_cmds.put("RPOP", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, LIST_WATCH));
		_cmds.put("RPOPLPUSH", 			new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, LIST_WATCH));
		_cmds.put("RPUSH", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, LIST_WATCH));
		_cmds.put("RPUSHX", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, LIST_WATCH));
		
		// Set
		_cmds.put("SADD", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, SET_WATCH));
		_cmds.put("SCARD", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SET_WATCH));
		_cmds.put("SISMEMBER", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SET_WATCH));
		_cmds.put("SMEMBERS", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SET_WATCH));
		_cmds.put("SMOVE", 				new RedisRequestPolicy(THROUGH_CMD, DELETE_CMD, SET_WATCH));
		_cmds.put("SPOP", 				new RedisRequestPolicy(THROUGH_CMD, DELETE_CMD, SET_WATCH));
		_cmds.put("SRANDMEMBER", 		new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SET_WATCH));
		_cmds.put("SREM", 				new RedisRequestPolicy(THROUGH_CMD, DELETE_CMD, SET_WATCH));
		_cmds.put("SSCAN", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SET_WATCH));
		
		_cmds.put("SDIFF", 				new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, SET_WATCH));
		_cmds.put("SDIFFSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, SET_WATCH));
		_cmds.put("SINTER", 			new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, SET_WATCH));
		_cmds.put("SINTERSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, SET_WATCH));
		_cmds.put("SUNION", 			new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, SET_WATCH));
		_cmds.put("SUNIONSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, SET_WATCH));
		
		
		// SortedSet
		_cmds.put("ZADD", 				new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, SORTED_SET_WATCH));
		_cmds.put("ZCARD", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZCOUNT", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZINCRBY", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, SORTED_SET_WATCH));
		_cmds.put("ZRANGE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZRANGEBYSCORE",  	new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZRANK", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZREM", 				new RedisRequestPolicy(THROUGH_CMD, DELETE_CMD, SORTED_SET_WATCH));
		_cmds.put("ZREMRANGEBYRANK",	new RedisRequestPolicy(THROUGH_CMD, DELETE_CMD, SORTED_SET_WATCH));
		_cmds.put("ZREMRANGEBYSCORE", 	new RedisRequestPolicy(THROUGH_CMD, DELETE_CMD, SORTED_SET_WATCH));
		_cmds.put("ZREVRANGE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZREVRANGEBYSCORE", 	new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZREVRANK", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZSCORE", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZUNIONSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, SORTED_SET_WATCH));
		_cmds.put("ZINTERSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, SORTED_SET_WATCH));
		_cmds.put("ZSCAN", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZRANGEBYLEX", 		new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZLEXCOUNT", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, SORTED_SET_WATCH));
		_cmds.put("ZREMRANGEBYLEX", 	new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, SORTED_SET_WATCH));
		
		// HyperLogLog
		_cmds.put("PFADD", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("PFCOUNT", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("PFMERGE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		
		// Geo
		_cmds.put("GEOADD", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("GEOPOS", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("GEODIST", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("GEORADIUS", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("GEORADIUSBYMEMBER", 	new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		_cmds.put("GEOHASH", 			new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		
		// Pub/Sub
		_cmds.put("PUBSUB", 			new RedisRequestPolicy(DISABLED_CMD, READ_CMD, NO_WATCH));		// 不支持
		_cmds.put("PUBLISH", 			new RedisRequestPolicy(THROUGH_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("PSUBSCRIBE", 		new RedisRequestPolicy(PUBSUB_CMD, READ_CMD, NO_WATCH));
		_cmds.put("PUNSUBSCRIBE", 		new RedisRequestPolicy(PUBSUB_CMD, READ_CMD, NO_WATCH));
		_cmds.put("SUBSCRIBE", 			new RedisRequestPolicy(PUBSUB_CMD, READ_CMD, NO_WATCH));
		_cmds.put("UNSUBSCRIBE", 		new RedisRequestPolicy(PUBSUB_CMD, READ_CMD, NO_WATCH));
		
		// Transaction
		_cmds.put("MULTI", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));	//开启事务
		_cmds.put("EXEC", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH)); //提交事务
		_cmds.put("DISCARD", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));	//取消事务
		_cmds.put("WATCH", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));	//监视
		_cmds.put("UNWATCH", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH)); //取消监视
		
		// Script 
		_cmds.put("EVAL", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("EVALSHA", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SCRIPT", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		
		// Connection
		_cmds.put("AUTH", 				new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, NO_WATCH));
		_cmds.put("ECHO", 				new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, NO_WATCH));
		_cmds.put("PING", 				new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, NO_WATCH));
		_cmds.put("QUIT", 				new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, NO_WATCH));
		_cmds.put("SELECT", 			new RedisRequestPolicy(AUTO_RESP_CMD, READ_CMD, NO_WATCH));
		
		// Server 
		_cmds.put("BGREWRITEAOF", 		new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("BGSAVE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("GETNAME", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));

		_cmds.put("CLIENT", 			new RedisRequestPolicy(NO_CLUSTER_CMD, WRITE_CMD, NO_WATCH));
		
		_cmds.put("SETNAME", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("CONFIG", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("RESETSTAT", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("REWRITE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("DBSIZE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("DEBUG", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SEGFAULT", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("FLUSHALL", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("FLUSHDB", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		
		_cmds.put("INFO", 				new RedisRequestPolicy(NO_CLUSTER_CMD, READ_CMD, NO_WATCH));
		
		_cmds.put("LASTSAVE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("MONITOR", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("PSYNC", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SAVE", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SHUTDOWN", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SLAVEOF", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SLOWLOG", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("SYNC", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("TIME", 				new RedisRequestPolicy(THROUGH_CMD, READ_CMD, NO_WATCH));
		
		_cmds.put("COMMAND", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));		// command count/getkeys/info/
		_cmds.put("WAIT", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));
		_cmds.put("ROLE", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));		
		_cmds.put("READONLY", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));		// 执行该命令后，可以在slave上执行只读命令
		_cmds.put("READWRITE", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));		// 执行该命令后，取消在slave上执行命令
		_cmds.put("TOUCH", 				new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));		// 
		_cmds.put("UNLINK", 			new RedisRequestPolicy(DISABLED_CMD, WRITE_CMD, NO_WATCH));		// 

	}
    
    // 解析特殊指令策略
	public static RedisRequestPolicy getPolicy(String cmd) {
		RedisRequestPolicy policy = _cmds.get( cmd );
		return policy == null ? new RedisRequestPolicy(UNKNOW_CMD, (byte)-1, (byte)-1) : policy;		
	}
	
	
}
