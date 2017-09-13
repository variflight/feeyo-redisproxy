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
	

	
	private static final Map<String, RedisRequestPolicy> _cmds = new HashMap<String, RedisRequestPolicy>();
	static {		
		// Manage
		_cmds.put("SHOW", 			new RedisRequestPolicy(RedisRequestPolicy.MANAGE_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("RELOAD", 		new RedisRequestPolicy(RedisRequestPolicy.MANAGE_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("JVM", 			new RedisRequestPolicy(RedisRequestPolicy.MANAGE_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZK", 			new RedisRequestPolicy(RedisRequestPolicy.MANAGE_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("NODE", 			new RedisRequestPolicy(RedisRequestPolicy.MANAGE_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("USE", 			new RedisRequestPolicy(RedisRequestPolicy.MANAGE_CMD, RedisRequestPolicy.WRITE_CMD));
		// Cluster
		_cmds.put("CLUSTER", 		new RedisRequestPolicy(RedisRequestPolicy.MANAGE_CMD, RedisRequestPolicy.WRITE_CMD));

		//Key
		_cmds.put("DEL", 			new RedisRequestPolicy(RedisRequestPolicy.DEL_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("DUMP", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));		//返回被序列化的值
		_cmds.put("EXISTS", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("EXPIRE", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("EXPIREAT", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("KEYS", 			new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("MIGRATE", 		new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("MOVE", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("OBJECT", 		new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("PERSIST", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));		//移除给定 key 的生存时间
		_cmds.put("PEXPIRE", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));		//它以毫秒为单位设置 key 的生存时间
		_cmds.put("PEXPIREAT", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD)); 		//以毫秒为单位设置 key 的过期 unix 时间戳
		_cmds.put("PTTL", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));	 	//以毫秒为单位返回 key 的剩余生存时间
		_cmds.put("RANDOMKEY", 		new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("RENAME", 		new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("RENAMENX", 		new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("RESTORE", 		new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));		//反序列化给定的序列化值，并将它和给定的 key 关联
		_cmds.put("SORT", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("TTL", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("TYPE", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SCAN", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		
		
		//String
		_cmds.put("APPEND", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("BITCOUNT", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("BITOP", 				new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("BITFIELD", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("DECR", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("DECRBY", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("GET", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("GETBIT", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("GETRANGE", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("GETSET", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("INCR", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("INCRBY", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("INCRBYFLOAT", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("MGET", 				new RedisRequestPolicy(RedisRequestPolicy.MGETSET_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("MSET", 				new RedisRequestPolicy(RedisRequestPolicy.MGETSET_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("MSETNX", 			new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("PSETEX", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));			//以毫秒为单位设置 key 的生存时间
		_cmds.put("SET", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SETBIT", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SETEX", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SETNX", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SETRANGE", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("STRLEN", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		
		//Hash
		_cmds.put("HDEL", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("HEXISTS", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("HGET", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("HGETALL", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("HINCRBY", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("HINCRBYFLOAT", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("HKEYS", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("HLEN", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("HMGET",				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("HMSET", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("HSET", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("HSETNX", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("HVALS", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("HSCAN", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("HSTRLEN", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		
		// List
		_cmds.put("BLPOP", 				new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("BRPOP", 				new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("BRPOPLPUSH", 		new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("LINDEX", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("LINSERT", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("LLEN", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("LPOP", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("LPUSH", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("LPUSHX", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("LRANGE", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("LREM", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("LSET", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("LTRIM", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("RPOP", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("RPOPLPUSH", 			new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("RPUSH", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("RPUSHX", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		
		// Set
		_cmds.put("SADD", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SCARD", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SISMEMBER", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SMEMBERS", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SMOVE", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SPOP", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SRANDMEMBER", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SREM", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SSCAN", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		
		_cmds.put("SDIFF", 				new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SDIFFSTORE", 		new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SINTER", 			new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SINTERSTORE", 		new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SUNION", 			new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SUNIONSTORE", 		new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		
		
		// SortedSet
		_cmds.put("ZADD", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("ZCARD", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZCOUNT", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZINCRBY", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("ZRANGE", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZRANGEBYSCORE",  	new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZRANK", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZREM", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("ZREMRANGEBYRANK",	new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("ZREMRANGEBYSCORE", 	new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("ZREVRANGE", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZREVRANGEBYSCORE", 	new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZREVRANK", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZSCORE", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZUNIONSTORE", 		new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("ZINTERSTORE", 		new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("ZSCAN", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("ZRANGEBYLEX", 		new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZLEXCOUNT", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ZREMRANGEBYLEX", 	new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		
		// HyperLogLog
		_cmds.put("PFADD", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("PFCOUNT", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("PFMERGE", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		
		// Geo
		_cmds.put("GEOADD", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("GEOPOS", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("GEODIST", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("GEORADIUS", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("GEORADIUSBYMEMBER", 	new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("GEOHASH", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		
		// Pub/Sub
		_cmds.put("PUBSUB", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.READ_CMD));		// 不支持
		_cmds.put("PUBLISH", 			new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("PSUBSCRIBE", 		new RedisRequestPolicy(RedisRequestPolicy.PUBSUB_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("PUNSUBSCRIBE", 		new RedisRequestPolicy(RedisRequestPolicy.PUBSUB_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SUBSCRIBE", 			new RedisRequestPolicy(RedisRequestPolicy.PUBSUB_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("UNSUBSCRIBE", 		new RedisRequestPolicy(RedisRequestPolicy.PUBSUB_CMD, RedisRequestPolicy.READ_CMD));
		
		// Transaction
		_cmds.put("MULTI", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));	//开启事务
		_cmds.put("EXEC", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD)); //提交事务
		_cmds.put("DISCARD", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));	//取消事务
		_cmds.put("WATCH", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));	//监视
		_cmds.put("UNWATCH", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD)); //取消监视
		
		// Script 
		_cmds.put("EVAL", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("EVALSHA", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SCRIPT", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		
		// Connection
		_cmds.put("AUTH", 				new RedisRequestPolicy(RedisRequestPolicy.AUTO_RESP_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("ECHO", 				new RedisRequestPolicy(RedisRequestPolicy.AUTO_RESP_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("PING", 				new RedisRequestPolicy(RedisRequestPolicy.AUTO_RESP_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("QUIT", 				new RedisRequestPolicy(RedisRequestPolicy.AUTO_RESP_CMD, RedisRequestPolicy.READ_CMD));
		_cmds.put("SELECT", 			new RedisRequestPolicy(RedisRequestPolicy.AUTO_RESP_CMD, RedisRequestPolicy.READ_CMD));
		
		// Server 
		_cmds.put("BGREWRITEAOF", 		new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("BGSAVE", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("GETNAME", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));

		_cmds.put("CLIENT", 			new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.WRITE_CMD));
		
		_cmds.put("SETNAME", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("CONFIG", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("RESETSTAT", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("REWRITE", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("DBSIZE", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("DEBUG", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SEGFAULT", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("FLUSHALL", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("FLUSHDB", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		
		_cmds.put("INFO", 				new RedisRequestPolicy(RedisRequestPolicy.NO_CLUSTER_CMD, RedisRequestPolicy.READ_CMD));
		
		_cmds.put("LASTSAVE", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("MONITOR", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("PSYNC", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SAVE", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SHUTDOWN", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SLAVEOF", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SLOWLOG", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("SYNC", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("TIME", 				new RedisRequestPolicy(RedisRequestPolicy.THROUGH_CMD, RedisRequestPolicy.READ_CMD));
		
		_cmds.put("COMMAND", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));		// command count/getkeys/info/
		_cmds.put("WAIT", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));
		_cmds.put("ROLE", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));		
		_cmds.put("READONLY", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));		// 执行该命令后，可以在slave上执行只读命令
		_cmds.put("READWRITE", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));		// 执行该命令后，取消在slave上执行命令
		_cmds.put("TOUCH", 				new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));		// 
		_cmds.put("UNLINK", 			new RedisRequestPolicy(RedisRequestPolicy.DISABLED_CMD, RedisRequestPolicy.WRITE_CMD));		// 

	}
    
    // 解析特殊指令策略
	public static RedisRequestPolicy getPolicy(String cmd) {
		RedisRequestPolicy policy = _cmds.get( cmd );
		return policy == null ? new RedisRequestPolicy(RedisRequestPolicy.UNKNOW_CMD, (byte)-1) : policy;		
	}
	
	
}
