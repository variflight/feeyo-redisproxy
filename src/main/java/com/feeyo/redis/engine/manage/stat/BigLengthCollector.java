package com.feeyo.redis.engine.manage.stat;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BigLengthCollector implements StatCollector {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BigLengthCollector.class );
	
	private final static int THRESHOLD = 10000;
	
	private final static int SIZE_OF_1K = 1024;
	private final static int SIZE_OF_10K = 10 * SIZE_OF_1K;
	
	
	// key -> password,cmd
	private static ConcurrentHashMap<String, String[]> keyMap = new ConcurrentHashMap<String, String[]>();
	
	private static ConcurrentHashMap<String, BigLength> bigLengthMap = new ConcurrentHashMap<String, BigLength>();
	
	private static long lastCheckTime = TimeUtil.currentTimeMillis();
	private static AtomicBoolean isChecking = new AtomicBoolean(false);

    private static ConcurrentHashMap<String, Long> errorMap = new ConcurrentHashMap<String, Long>();

	/**
	 * 检查 redis key
	 */
	private void checkListKeyLength() {
		
		if ( !isChecking.compareAndSet(false, true) ) {
			return;
		}
		
		try {

			lastCheckTime = TimeUtil.currentTimeMillis();
			
			
			for (java.util.Map.Entry<String, String[]>  listKey : keyMap.entrySet()) {
				
				String key = listKey.getKey();
				String[] value = listKey.getValue();
				String password = value[0];
				String cmd = value[1];
				
				UserCfg userCfg = RedisEngineCtx.INSTANCE().getUserMap().get( password );
				
				if (userCfg != null) {
					AbstractPool pool = RedisEngineCtx.INSTANCE().getPoolMap().get( userCfg.getPoolId() );
					PhysicalNode physicalNode;
					if (pool.getType() == 1) {
						physicalNode = pool.getPhysicalNode(cmd, key);
					} else {
						physicalNode = pool.getPhysicalNode();
					}
					
					JedisConnection conn = null;		
					try {
						
						// 前置设置 readonly
						conn = new JedisConnection(physicalNode.getHost(), physicalNode.getPort(), 1000, 0);
						
						if ( cmd.equals("HMSET") 	
								|| cmd.equals("HSET") 	
								|| cmd.equals("HSETNX") 	
								|| cmd.equals("HINCRBY") 	
								|| cmd.equals("HINCRBYFLOAT") 	) { // hash
							
							conn.sendCommand( RedisCommand.HLEN, key );
							
						} else if (cmd.equals("LPUSH") 	
								|| cmd.equals("LPUSHX") 
								|| cmd.equals("RPUSH") 
								|| cmd.equals("RPUSHX") ) { // list
							
							conn.sendCommand( RedisCommand.LLEN, key );
							
						} else if(  cmd.equals("SADD") ) {  // set
								
								conn.sendCommand( RedisCommand.SCARD, key );
							
						} else if ( cmd.equals("ZADD")  
								|| cmd.equals("ZINCRBY") 
								|| cmd.equals("ZREMRANGEBYLEX")) { // sortedset
							
							conn.sendCommand( RedisCommand.ZCARD, key );
						}
						
						// 获取集合长度
						long length = conn.getIntegerReply();
						if ( length > THRESHOLD ) {
							
							BigLength bigLen = bigLengthMap.get(key);
							if ( bigLen == null ) {
								bigLen = new BigLength();
								bigLen.cmd = cmd;
								bigLen.key = key;
								bigLen.password = password;
								bigLengthMap.put(key, bigLen);
							}
							bigLen.length.set( (int)length );
							
						} else {
							bigLengthMap.remove( key );
						}
						
						keyMap.remove(key);
						
						//###########################################
						if (bigLengthMap.size() > 100) {
							BigLength min = null;
							for (BigLength bigLen : bigLengthMap.values()) {
								if ( min == null ) {
									min = bigLen;
								} else {
									if (bigLen.length.get() < min.length.get()) {
										min = bigLen;
									}
								}
							}
							bigLengthMap.remove( min.key );
						}
						
						
					} catch (Exception e2) {
						//
                        Long lastTime = errorMap.get(physicalNode.getHost());
                        if (lastTime == null || ((lastCheckTime - lastTime) > 30000)) {
                            LOGGER.error("Failed to connecting {}:{}", physicalNode.getHost(), physicalNode.getPort());
                            errorMap.put(physicalNode.getHost(), lastCheckTime);
                        }

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

	    
	public ConcurrentHashMap<String, BigLength> getBigLengthMap() {
		return bigLengthMap;
	}
	
	

	
	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, 
			int procTimeMills, int waitTimeMills, boolean isCommandOnly, boolean isBypass ) {
		
		// 统计集合类型key
		if (  	cmd.equals("HMSET") 	// hash
				|| cmd.equals("HSET") 	
				|| cmd.equals("HSETNX") 	
				|| cmd.equals("HINCRBY") 	
				|| cmd.equals("HINCRBYFLOAT") 	
				
				|| cmd.equals("LPUSH") 	// list
				|| cmd.equals("LPUSHX") 
				|| cmd.equals("RPUSH") 
				|| cmd.equals("RPUSHX") 
				
				|| cmd.equals("SADD") // set
				
				|| cmd.equals("ZADD")  // sortedset
				|| cmd.equals("ZINCRBY") 
				|| cmd.equals("ZREMRANGEBYLEX") ) {
		
	
				BigLength bigLength = bigLengthMap.get(key);
				if (bigLength != null) {
					if (requestSize > SIZE_OF_10K ) {
						bigLength.count_10k.incrementAndGet();
						
					} else if (requestSize > SIZE_OF_1K) {
						bigLength.count_1k.incrementAndGet();
					}
				}
				
				if ( !keyMap.containsKey(key) ) {
					
					if (keyMap.size() < 1000) {;
						keyMap.put(key, new String[]{ password, cmd});
					} else {
						checkListKeyLength();
					}
				}
		}
		
	}
	
	
	@Override
	public void onScheduleToZore() {
		ConcurrentHashMap<String, String[]> tmp = new ConcurrentHashMap<String, String[]>();
		for (BigLength bigLength : bigLengthMap.values()) { 
			tmp.put(bigLength.key, new String[] { bigLength.password, bigLength.cmd} );
		}
		
		keyMap.putAll(tmp);
	}
	
	@Override
	public void onSchedulePeroid(int peroid) {
		if (TimeUtil.currentTimeMillis() - lastCheckTime >= peroid * 1000 ) {
			checkListKeyLength();
        }
	}
	
	public static class BigLength {
		public String password;
		public String cmd;
		public String key;
		public AtomicInteger length = new AtomicInteger(0);
		public AtomicInteger count_1k = new AtomicInteger(0);
		public AtomicInteger count_10k = new AtomicInteger(0);
	}
}
