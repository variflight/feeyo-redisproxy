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
import com.feeyo.redis.net.backend.pool.cluster.ClusterCRC16Util;
import com.feeyo.redis.net.backend.pool.cluster.RedisClusterPool;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;
import com.feeyo.util.jedis.exception.JedisDataException;

public class CollectionKey implements StatListener {
	
	private static Logger LOGGER = LoggerFactory.getLogger( CollectionKey.class );
	
	private final static int COLLECTION_KEY_LENGTH_THRESHOLD = 10000;
	
	private final static int MIN_WATCH_LEN = 500;
	private final static int MAX_WATCH_LEN = 1000;
	
	// 缓存最近出现过的集合类型key
	private static ConcurrentHashMap<String, CollectionKey> collectionKeyBuffer = new ConcurrentHashMap<String, CollectionKey>();
	private static ConcurrentHashMap<String, CollectionKey> collectionKeyTop100OfLength = new ConcurrentHashMap<String, CollectionKey>();
	
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
			
			for (Entry<String, CollectionKey> entry : collectionKeyBuffer.entrySet()) {
				
				CollectionKey collectionKey = entry.getValue();
				String user = collectionKey.user;
				UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get(user);
				
				if (userCfg != null) {
					PhysicalNode physicalNode = null;
					AbstractPool pool = RedisEngineCtx.INSTANCE().getPoolMap().get( userCfg.getPoolId() );
					// 单节点
					if (pool.getType() == 0) {
						physicalNode = pool.getPhysicalNode();
						
					// 集群池
					} else if (pool.getType() == 1) {
						RedisClusterPool clusterPool = (RedisClusterPool) pool;
						// 计算key的slot值。
						int slot = ClusterCRC16Util.getSlot(collectionKey.key);
						// 根据 slot 获取 redis物理节点
						physicalNode = clusterPool.getPhysicalNodeBySlot(slot);
					}
					
					JedisConnection conn = null;		
					try {
						
						String host = physicalNode.getHost();
						int port = physicalNode.getPort();	
						
						conn = new JedisConnection(host, port, 1000, 0);
						
						if ( cmd.equals("HMSET") 	
								|| cmd.equals("HSET") 	
								|| cmd.equals("HSETNX") 	
								|| cmd.equals("HINCRBY") 	
								|| cmd.equals("HINCRBYFLOAT") 	) { // hash
							
							conn.sendCommand( RedisCommand.HLEN, collectionKey.key );
							
						} else if (cmd.equals("LPUSH") 	
								|| cmd.equals("LPUSHX") 
								|| cmd.equals("RPUSH") 
								|| cmd.equals("RPUSHX") ) { // list
							
							conn.sendCommand( RedisCommand.LLEN, collectionKey.key );
							
						} else if(  cmd.equals("SADD") ) {  // set
								
								conn.sendCommand( RedisCommand.SCARD, collectionKey.key );
							
						} else if ( cmd.equals("ZADD")  
								|| cmd.equals("ZINCRBY") 
								|| cmd.equals("ZREMRANGEBYLEX")) { // sortedset
							
							conn.sendCommand( RedisCommand.ZCARD, collectionKey.key );
						}
						
		
						long length = conn.getIntegerReply();
						if ( length > COLLECTION_KEY_LENGTH_THRESHOLD ) {
							collectionKey.length.set((int) length);
							addCollectionKeyToTop100(collectionKey);
						} else {

							collectionKeyTop100OfLength.remove(collectionKey.key);
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

	    
	    public  void addCollectionKeyToTop100 (CollectionKey collectionKey) {
	    	if (collectionKeyTop100OfLength.get(collectionKey.key) != null) {
	    		CollectionKey ck = collectionKeyTop100OfLength.get(collectionKey.key);
	    		ck.length = collectionKey.length;
	    	} else if (collectionKeyTop100OfLength.size() > 100) {
	    		CollectionKey currentCollectionKey = collectionKey;
	    		for (Entry<String, CollectionKey> entry : collectionKeyTop100OfLength.entrySet()) {
	    			CollectionKey ck = entry.getValue();
	    			if (ck.length.get() < currentCollectionKey.length.get()) {
	    				currentCollectionKey = ck;
	    			}
	    		}
	    		if (collectionKeyTop100OfLength.remove(currentCollectionKey.key) != null)
	    			collectionKeyTop100OfLength.put(collectionKey.key, collectionKey);
	    	} else {
	    		collectionKeyTop100OfLength.put(collectionKey.key, collectionKey);
	    	}
	    }
	    
	    public  Set<Entry<String, CollectionKey>> getCollectionKeyTop100OfLength() {
	    	return collectionKeyTop100OfLength.entrySet();
	    }
	    
	
	
	@Override
	public void onBigKey(String user, String cmd, String key, int requestSize, int responseSize) {
	}
	
	@Override
	public void onCollectionKey(String password, String cmd, String key, int requestSize) {
	
		// 如果是写入指令，并且此数据是集合类key长度top100中。判断此次请求的数据大小，记录，用于估算大长度key的数据大小。
		CollectionKey ck = collectionKeyTop100OfLength.get(key);
		if (ck != null) {
			if (requestSize > 10 * 1024) {
				ck.count_10k.incrementAndGet();
			} else if (requestSize > 1024) {
				ck.count_1k.incrementAndGet();
			}
		}
		
		CollectionKey cKey = collectionKeyBuffer.get(key);
		if (cKey == null) {
			if (collectionKeyBuffer.size() < MAX_WATCH_LEN) {
				cKey = new CollectionKey();
				cKey.cmd = cmd;
				cKey.key = key;
				cKey.user = password;
				collectionKeyBuffer.put(key, cKey);
			}
			if (collectionKeyBuffer.size() >= MIN_WATCH_LEN 
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
