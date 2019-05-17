package com.feeyo.redis.net.front.handler;

import com.feeyo.net.codec.redis.RedisRequestPolicy;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author zhuam
 *
 */
public class CommandParse {
	
	public static final byte UNKNOW_CMD   		= -1;	// 未知指令
	
	// 识别策略
	public static final byte DISABLED_CMD 		= 0;		// 禁止指令
	public static final byte NO_CLUSTER_CMD 	= 1;		// 非集群指令
	public static final byte COMMON_CMD 		= 2;		// 通用指令
	public static final byte MANAGE_CMD 		= 3;		// 管理指令
	public static final byte KAFKA_CMD 			= 4;		// kafka指令
	
	// 处理策略 THROUGH
	public static final byte NO_THROUGH_CMD		= 7;		// 中间件不透传指令
	public static final byte THROUGH_CMD 		= 8;		// 中间件透传指令
	public static final byte PUBSUB_CMD 		= 9;		// 中间件特殊处理 pubsub
	public static final byte MGETSET_CMD		= 10;		// 中间件加强指令 mgetset
	public static final byte DEL_CMD			= 11;		// 中间件加强指令 del
	public static final byte EXISTS_CMD			= 12;		// 中间件加强指令 exists
	public static final byte BLOCK_CMD 	    	= 13;		// 中间件加强指令, 阻塞指令特殊处理
	
	// 处理策略 kakfa指令
	public static final byte PRODUCE_CMD    	= 14;       // 生产指令
	public static final byte CONSUMER_CMD    	= 15;       // 消费指令
	public static final byte PARTITIONS_CMD    	= 16;       // 获取分区指令
	public static final byte OFFSET_CMD    	= 17;       // 获取点位指令
	public static final byte PRIVATE_CMD    	= 18;       // 内部调用指令
	
	
	// RW 
	public static final byte WRITE_CMD = 1;
	public static final byte READ_CMD = 2;
	
	
	private static final Map<String, RedisRequestPolicy> _cmds = new HashMap<String, RedisRequestPolicy>( 220 );
	
	static {	
		
		// Manage
		_cmds.put("SHOW", 				new RedisRequestPolicy(MANAGE_CMD, NO_THROUGH_CMD, READ_CMD));
		_cmds.put("RELOAD", 			new RedisRequestPolicy(MANAGE_CMD, NO_THROUGH_CMD, WRITE_CMD));
		_cmds.put("JVM", 				new RedisRequestPolicy(MANAGE_CMD, NO_THROUGH_CMD, READ_CMD));
		_cmds.put("NODE", 				new RedisRequestPolicy(MANAGE_CMD, NO_THROUGH_CMD, WRITE_CMD));
		_cmds.put("USE", 				new RedisRequestPolicy(MANAGE_CMD, NO_THROUGH_CMD, WRITE_CMD));
		_cmds.put("REPAIR", 			new RedisRequestPolicy(MANAGE_CMD, NO_THROUGH_CMD, WRITE_CMD));
		
		// kafka
		_cmds.put("KPUSH", 				new RedisRequestPolicy(KAFKA_CMD, PRODUCE_CMD, WRITE_CMD));
		_cmds.put("KPOP", 				new RedisRequestPolicy(KAFKA_CMD, CONSUMER_CMD, WRITE_CMD));
		_cmds.put("KPARTITIONS", 		new RedisRequestPolicy(KAFKA_CMD, PARTITIONS_CMD, READ_CMD));
		_cmds.put("KOFFSET", 			new RedisRequestPolicy(KAFKA_CMD, OFFSET_CMD, READ_CMD));
		
		//
		// kafka private
		_cmds.put("KGETOFFSET", 		new RedisRequestPolicy(KAFKA_CMD, PRIVATE_CMD, WRITE_CMD));
		_cmds.put("KRETURNOFFSET", 		new RedisRequestPolicy(KAFKA_CMD, PRIVATE_CMD, WRITE_CMD));
		
		// Cluster
		_cmds.put("CLUSTER", 			new RedisRequestPolicy(MANAGE_CMD, NO_THROUGH_CMD, WRITE_CMD));

        // Print
        _cmds.put("PRINT", 			new RedisRequestPolicy(MANAGE_CMD, NO_THROUGH_CMD, WRITE_CMD));

		//Key
		_cmds.put("DEL", 				new RedisRequestPolicy(COMMON_CMD, DEL_CMD, WRITE_CMD));
		_cmds.put("DUMP", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));		//返回被序列化的值
		_cmds.put("EXISTS", 			new RedisRequestPolicy(COMMON_CMD, EXISTS_CMD, READ_CMD));
		_cmds.put("EXPIRE", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("EXPIREAT", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("KEYS", 				new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("MIGRATE", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("MOVE", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("OBJECT", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("PERSIST", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));		//移除给定 key 的生存时间
		_cmds.put("PEXPIRE", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));		//它以毫秒为单位设置 key 的生存时间
		_cmds.put("PEXPIREAT", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD)); 		//以毫秒为单位设置 key 的过期 unix 时间戳
		_cmds.put("PTTL", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));	 	//以毫秒为单位返回 key 的剩余生存时间
		_cmds.put("RANDOMKEY", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("RENAME", 			new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("RENAMENX", 			new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("RESTORE", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));		//反序列化给定的序列化值，并将它和给定的 key 关联
		_cmds.put("SORT", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("TTL", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("TYPE", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("SCAN", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, READ_CMD));
		
		
		//String
		_cmds.put("APPEND", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("BITCOUNT", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("BITOP", 				new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("BITFIELD", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("DECR", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("DECRBY", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("GET", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("GETBIT", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("GETRANGE", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("GETSET", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("INCR", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("INCRBY", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("INCRBYFLOAT", 		new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("MGET", 				new RedisRequestPolicy(COMMON_CMD, MGETSET_CMD, READ_CMD));
		_cmds.put("MSET", 				new RedisRequestPolicy(COMMON_CMD, MGETSET_CMD, WRITE_CMD));
		_cmds.put("MSETNX", 			new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("PSETEX", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));			//以毫秒为单位设置 key 的生存时间
		_cmds.put("SET", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SETBIT", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SETEX", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SETNX", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SETRANGE", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("STRLEN", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		
		//Hash
		_cmds.put("HDEL", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("HEXISTS", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("HGET", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("HGETALL", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("HINCRBY", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("HINCRBYFLOAT", 		new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("HKEYS", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("HLEN", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("HMGET",				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("HMSET", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("HSET", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("HSETNX", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("HVALS", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("HSCAN", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("HSTRLEN", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		
		// List
		_cmds.put("BLPOP", 				new RedisRequestPolicy(NO_CLUSTER_CMD, BLOCK_CMD, READ_CMD));
		_cmds.put("BRPOP", 				new RedisRequestPolicy(NO_CLUSTER_CMD, BLOCK_CMD, READ_CMD));
		_cmds.put("BRPOPLPUSH", 		new RedisRequestPolicy(NO_CLUSTER_CMD, BLOCK_CMD, WRITE_CMD));
		
		_cmds.put("LINDEX", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("LINSERT", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("LLEN", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("LPOP", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("LPUSH", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("LPUSHX", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("LRANGE", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("LREM", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("LSET", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("LTRIM", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("RPOP", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		
		_cmds.put("RPOPLPUSH", 			new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		
		_cmds.put("RPUSH", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("RPUSHX", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		
		// Set
		_cmds.put("SADD", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SCARD", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("SISMEMBER", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("SMEMBERS", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("SMOVE", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SPOP", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SRANDMEMBER", 		new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("SREM", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SSCAN", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		
		_cmds.put("SDIFF", 				new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("SDIFFSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SINTER", 			new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("SINTERSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SUNION", 			new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("SUNIONSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		
		
		// SortedSet
		_cmds.put("ZADD", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("ZCARD", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZCOUNT", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZINCRBY", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("ZRANGE", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZRANGEBYSCORE",  	new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZRANK", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZREM", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("ZREMRANGEBYRANK",	new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("ZREMRANGEBYSCORE", 	new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("ZREVRANGE", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZREVRANGEBYSCORE", 	new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZREVRANK", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZSCORE", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZUNIONSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("ZINTERSTORE", 		new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("ZSCAN", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZRANGEBYLEX", 		new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZLEXCOUNT", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("ZREMRANGEBYLEX", 	new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		
		// HyperLogLog
		_cmds.put("PFADD", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("PFCOUNT", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("PFMERGE", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		
		// Geo
		_cmds.put("GEOADD", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("GEOPOS", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("GEODIST", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("GEORADIUS", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("GEORADIUSBYMEMBER", 	new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		_cmds.put("GEOHASH", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		
		// Pub/Sub
		_cmds.put("PUBSUB", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, READ_CMD));		// 不支持
		_cmds.put("PUBLISH", 			new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("PSUBSCRIBE", 		new RedisRequestPolicy(COMMON_CMD, PUBSUB_CMD, READ_CMD));
		_cmds.put("PUNSUBSCRIBE", 		new RedisRequestPolicy(COMMON_CMD, PUBSUB_CMD, READ_CMD));
		_cmds.put("SUBSCRIBE", 			new RedisRequestPolicy(COMMON_CMD, PUBSUB_CMD, READ_CMD));
		_cmds.put("UNSUBSCRIBE", 		new RedisRequestPolicy(COMMON_CMD, PUBSUB_CMD, READ_CMD));
		
		// Transaction
		_cmds.put("MULTI", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));	//开启事务
		_cmds.put("EXEC", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD)); //提交事务
		_cmds.put("DISCARD", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));	//取消事务
		_cmds.put("WATCH", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));	//监视
		_cmds.put("UNWATCH", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD)); //取消监视
		
		// Script 
		_cmds.put("EVAL", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, WRITE_CMD)); // NO_CLUSTER_CMD
		_cmds.put("EVALSHA", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SCRIPT", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		
		// Connection
		_cmds.put("AUTH", 				new RedisRequestPolicy(COMMON_CMD, NO_THROUGH_CMD, READ_CMD));
		_cmds.put("ECHO", 				new RedisRequestPolicy(COMMON_CMD, NO_THROUGH_CMD, READ_CMD));
		_cmds.put("PING", 				new RedisRequestPolicy(COMMON_CMD, NO_THROUGH_CMD, READ_CMD));
		_cmds.put("QUIT", 				new RedisRequestPolicy(COMMON_CMD, NO_THROUGH_CMD, READ_CMD));
		_cmds.put("SELECT", 			new RedisRequestPolicy(COMMON_CMD, NO_THROUGH_CMD, READ_CMD));
		
		// Server 
		_cmds.put("BGREWRITEAOF", 		new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("BGSAVE", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("GETNAME", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));

		_cmds.put("CLIENT", 			new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, WRITE_CMD));
		
		_cmds.put("SETNAME", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("CONFIG", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("RESETSTAT", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("REWRITE", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("DBSIZE", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("DEBUG", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SEGFAULT", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("FLUSHALL", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("FLUSHDB", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		
		_cmds.put("INFO", 				new RedisRequestPolicy(NO_CLUSTER_CMD, THROUGH_CMD, READ_CMD));
		
		_cmds.put("LASTSAVE", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("MONITOR", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("PSYNC", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SAVE", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SHUTDOWN", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SLAVEOF", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SLOWLOG", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("SYNC", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("TIME", 				new RedisRequestPolicy(COMMON_CMD, THROUGH_CMD, READ_CMD));
		
		_cmds.put("COMMAND", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));		// command count/getkeys/info/
		_cmds.put("WAIT", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));
		_cmds.put("ROLE", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));		
		_cmds.put("READONLY", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));		// 执行该命令后，可以在slave上执行只读命令
		_cmds.put("READWRITE", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));		// 执行该命令后，取消在slave上执行命令
		_cmds.put("TOUCH", 				new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));		// 
		_cmds.put("UNLINK", 			new RedisRequestPolicy(DISABLED_CMD, THROUGH_CMD, WRITE_CMD));		// 

	}
    
    // 解析特殊指令策略
	public static RedisRequestPolicy getPolicy(String cmd) {
		RedisRequestPolicy policy = _cmds.get( cmd );
		return policy == null ? new RedisRequestPolicy(UNKNOW_CMD, UNKNOW_CMD, UNKNOW_CMD) : policy;		
	}
}


