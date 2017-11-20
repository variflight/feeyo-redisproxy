package com.feeyo.redis.engine.manage.stat;

import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;
import com.feeyo.util.jedis.exception.JedisDataException;

public class CollectionKey implements StatListener {
	
	private static Logger LOGGER = LoggerFactory.getLogger( CollectionKey.class );
	
	private final static int LENGTH_THRESHOLD = 10000;
	
	private final static int MIN_WATCH_LEN = 500;
	private final static int MAX_WATCH_LEN = 1000;
	
	// 缓存最近出现过的集合类型key
	private static ConcurrentHashMap<String, CollectionKey> ckeyMap = new ConcurrentHashMap<String, CollectionKey>();
	private static ConcurrentHashMap<String, CollectionKey> ckeyTop100Map = new ConcurrentHashMap<String, CollectionKey>();
	
	public String user;
	public String cmd;
	public String key;
	public AtomicInteger length = new AtomicInteger(0);
	public AtomicInteger count_1k = new AtomicInteger(0);
	public AtomicInteger count_10k = new AtomicInteger(0);
	
	private static long lastCheckTime = TimeUtil.currentTimeMillis();
	private static AtomicBoolean isChecking = new AtomicBoolean(false);
	
	

	/**
	 * 检查 redis key
	 */
	private void checkRedisCollectionKey() {
		
		if ( !isChecking.compareAndSet(false, true) ) {
			return;
		}
		
		try {

			lastCheckTime = TimeUtil.currentTimeMillis();
			
			for (Entry<String, CollectionKey> entry : ckeyMap.entrySet()) {
				
				CollectionKey cKey = entry.getValue();
				String user = cKey.user;
				UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get(user);
				
				if (userCfg != null) {
					AbstractPool pool = RedisEngineCtx.INSTANCE().getPoolMap().get( userCfg.getPoolId() );
					PhysicalNode physicalNode = pool.getPhysicalNode();
					
					JedisConnection conn = null;		
					try {
						
						// 前置设置 readonly
						conn = new JedisConnection(physicalNode.getHost(), physicalNode.getPort(), 1000, 0);
						conn.sendCommand( RedisCommand.READONLY );
						conn.getBulkReply();
						
						if ( cmd.equals("HMSET") 	
								|| cmd.equals("HSET") 	
								|| cmd.equals("HSETNX") 	
								|| cmd.equals("HINCRBY") 	
								|| cmd.equals("HINCRBYFLOAT") 	) { // hash
							
							conn.sendCommand( RedisCommand.HLEN, cKey.key );
							
						} else if (cmd.equals("LPUSH") 	
								|| cmd.equals("LPUSHX") 
								|| cmd.equals("RPUSH") 
								|| cmd.equals("RPUSHX") ) { // list
							
							conn.sendCommand( RedisCommand.LLEN, cKey.key );
							
						} else if(  cmd.equals("SADD") ) {  // set
								
								conn.sendCommand( RedisCommand.SCARD, cKey.key );
							
						} else if ( cmd.equals("ZADD")  
								|| cmd.equals("ZINCRBY") 
								|| cmd.equals("ZREMRANGEBYLEX")) { // sortedset
							
							conn.sendCommand( RedisCommand.ZCARD, cKey.key );
						}
						
						// 获取集合长度
						long length = conn.getIntegerReply();
						if ( length > LENGTH_THRESHOLD ) {
							
							cKey.length.set((int) length);

							if (ckeyTop100Map.get(cKey.key) != null) {
								CollectionKey ck = ckeyTop100Map.get(cKey.key);
								ck.length = cKey.length;
								
							} else if (ckeyTop100Map.size() > 100) {
								
								CollectionKey minCKey = null;
								for (Entry<String, CollectionKey> entry1 : ckeyTop100Map.entrySet()) {
									CollectionKey ck = entry1.getValue();
									if ( minCKey == null ) {
										minCKey = ck;
									} else {
										if (ck.length.get() < minCKey.length.get()) {
											minCKey = ck;
										}
									}
								}
								
								if ( minCKey != null)
									ckeyTop100Map.remove(minCKey.key);
								
							} else {
								ckeyTop100Map.put(cKey.key, cKey);
							}

						} else {

							ckeyTop100Map.remove(cKey.key);
						}
						
					} catch (JedisDataException e1) {
					} catch (JedisConnectionException e2) {
						LOGGER.error("", e2);	
					} finally {
						if ( conn != null ) {
							conn.disconnect();
						}
					}
					
				}
			}
			
		} finally {
			isChecking.set(false);
		}
	}

	    
	    public  Set<Entry<String, CollectionKey>> getCollectionKeyTop100OfLength() {
	    	return ckeyTop100Map.entrySet();
	    }
	    
	
	
	@Override
	public void onBigKey(String user, String cmd, String key, int requestSize, int responseSize) {
	}
	
	@Override
	public void onCollectionKey(String password, String cmd, String key, int requestSize) {
	
		// 如果是写入指令，并且此数据是集合类key长度top100中。判断此次请求的数据大小，记录，用于估算大长度key的数据大小。
		CollectionKey ck = ckeyTop100Map.get(key);
		if (ck != null) {
			if (requestSize > 10 * 1024) {
				ck.count_10k.incrementAndGet();
			} else if (requestSize > 1024) {
				ck.count_1k.incrementAndGet();
			}
		}
		
		CollectionKey cKey = ckeyMap.get(key);
		if (cKey == null) {
			if (ckeyMap.size() < MAX_WATCH_LEN) {
				cKey = new CollectionKey();
				cKey.cmd = cmd;
				cKey.key = key;
				cKey.user = password;
				ckeyMap.put(key, cKey);
			}
			if (ckeyMap.size() >= MIN_WATCH_LEN 
					&& isChecking.compareAndSet(false, true)) 
				checkRedisCollectionKey();
		}
		
	}
	
	
	@Override
	public void onScheduleToZore() {
	}
	
	@Override
	public void onSchedulePeroid(int peroid) {
		
		if (TimeUtil.currentTimeMillis() - lastCheckTime >= peroid * 1000 ) {
			checkRedisCollectionKey();
        }
	}
}
