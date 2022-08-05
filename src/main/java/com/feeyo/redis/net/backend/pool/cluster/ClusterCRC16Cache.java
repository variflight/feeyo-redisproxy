package com.feeyo.redis.net.backend.pool.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class ClusterCRC16Cache {
	
	/**
	 * TODO: CRC16 计算耗CPU, 此处对  list/ hash 指令的 slot 做 cache 
	 */	
	private static LoadingCache<String, Integer> cache = CacheBuilder.newBuilder()
			.maximumSize( 9000 ) //
			.expireAfterWrite(30, TimeUnit.MINUTES) //
			.build(new CacheLoader<String, Integer>(){
				 @Override
	             public Integer load(String key) throws Exception {
					int slot = ClusterCRC16Util.getSlot( key );
					cache.put(key, slot);
					return slot;
				 }				
			});
	
	private static Map<String, Byte> cmds = new HashMap<String, Byte>();
	private static final byte B1 = 1;
	
	static {		
		// hash
		cmds.put("HDEL", B1);
		cmds.put("HEXISTS", B1);
		cmds.put("HGET", B1);
		cmds.put("HGETALL", B1);
		cmds.put("HINCRBY", B1);
		cmds.put("HINCRBYFLOAT", B1);
		cmds.put("HKEYS", B1);
		cmds.put("HLEN", B1);
		cmds.put("HMGET", B1);
		cmds.put("HMSET", B1);
		cmds.put("HSET", B1);
		cmds.put("HSETNX", B1);
		cmds.put("HVALS", B1);
		cmds.put("HSCAN", B1);
		
		// list
		cmds.put("BLPOP", B1);
		cmds.put("BRPOP", B1);
		cmds.put("BRPOPLPUSH", B1);
		cmds.put("LINDEX", B1);
		cmds.put("LINSERT", B1);
		cmds.put("LLEN", B1);
		cmds.put("LPOP", B1);
		cmds.put("LPUSH", B1);
		cmds.put("LPUSHX", B1);
		cmds.put("LRANGE", B1);
		cmds.put("LREM", B1);
		cmds.put("LSET", B1);
		cmds.put("LTRIM", B1);
		cmds.put("RPOP", B1);
		cmds.put("RPOPLPUSH", B1);
		cmds.put("RPUSH", B1);
		cmds.put("RPUSHX", B1);
	}
	
	public int getSlot(String cmd, String key) {
		if ( key == null )
			return 0;
		
		int slot = 0;
		Byte isCached = cmds.get( cmd );
		if ( isCached != null && isCached == B1  ) {
			try {				
				slot = cache.get( key );
			} catch (Exception e) {
				slot = ClusterCRC16Util.getSlot( key );
			}			
		} else {
			slot = ClusterCRC16Util.getSlot( key );		
		}
		return slot;
	}
	
	public static void main(String[] args) {
		ClusterCRC16Cache cache = new ClusterCRC16Cache();
		System.out.println(  cache.getSlot("HGET", "k1") );
		System.out.println(  cache.getSlot("HGET", "k1") );		
	}
	
}