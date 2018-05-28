package com.feeyo.kafka.net.backend.broker.offset;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.JedisPool;
import com.feeyo.util.jedis.RedisCommand;

public class RemoteOffsetAdmin {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RemoteOffsetAdmin.class );
	
	private JedisHolder jedisHolder = new JedisHolder();
	
	// 获取offset
	public long getOffset(String remoteAddress, String user, String topic, int partition) {
		long offset = -1;
		JedisPool jedisPool = jedisHolder.getJedisPool(remoteAddress);
		JedisConnection conn = jedisPool.getResource();
		try {
			conn.sendCommand(RedisCommand.AUTH, user);
			conn.getStatusCodeReply();
			
			conn.sendCommand(RedisCommand.KGETOFFSET, topic, String.valueOf(partition));
			String str = conn.getStatusCodeReply();
			offset = Long.parseLong(str);
			
		} catch (Exception e) {
			LOGGER.error("get offset err:", e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		
		return offset;
	}
	
	// 返还 offset
	public String returnOffset(String remoteAddress, String user, String topic, int partition, long offset) {
		JedisPool jedisPool = jedisHolder.getJedisPool(remoteAddress);
		JedisConnection conn = jedisPool.getResource();
		try {
			conn.sendCommand(RedisCommand.AUTH, user);
			conn.getStatusCodeReply();
			
			conn.sendCommand(RedisCommand.KRETURNOFFSET, topic, String.valueOf(partition), String.valueOf(offset));
			return conn.getStatusCodeReply();
			
		} catch (Exception e) {
			LOGGER.error("return offset err:", e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		
		return null;
	}
	
	
	//
	class JedisHolder {
		
		private ConcurrentHashMap<String, JedisPool> holder = new ConcurrentHashMap<>();
		
		// 连接池中最大空闲的连接数
		private int maxIdle = 50;
		private int minIdle = 10;
		// 当调用borrow Object方法时，是否进行有效性检查
		private boolean testOnBorrow = false;
		// 当调用return Object方法时，是否进行有效性检查
		private boolean testOnReturn = false;
		// 如果为true，表示有一个idle object evitor线程对idle
		// object进行扫描，如果validate失败，此object会被从pool中drop掉
		// TODO: 这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
		private boolean testWhileIdle = true;
		// 对于“空闲链接”检测线程而言，每次检测的链接资源的个数.(jedis 默认设置成-1)
		private int numTestsPerEvictionRun = -1;
		// 连接空闲的最小时间，达到此值后空闲连接将可能会被移除。负值(-1)表示不移除
		private int minEvictableIdleTimeMillis = 60 * 1000;
		// “空闲链接”检测线程，检测的周期，毫秒数。如果为负值，表示不运行“检测线程”。默认为-1
		private int timeBetweenEvictionRunsMillis = 30 * 1000;
		
		public JedisPool getJedisPool(String address) {
			
			JedisPool jedisPool = holder.get(address);
			if ( jedisPool == null ) {
				synchronized (this) {
					jedisPool = holder.get(address);
					if ( jedisPool == null) {
						String[] strs = address.split(":");
						jedisPool = initJedisPool(strs[0], Integer.parseInt(strs[1]));
						holder.put(address, jedisPool);
					} 
				}
			}
			return jedisPool;
		}
		
		private JedisPool initJedisPool(String host, int port) {

			GenericObjectPoolConfig jedisPoolConfig = new GenericObjectPoolConfig();
			jedisPoolConfig.setMaxIdle(maxIdle);
			jedisPoolConfig.setMinIdle(minIdle);
			jedisPoolConfig.setTestOnBorrow(testOnBorrow);
			jedisPoolConfig.setTestOnReturn(testOnReturn);
			jedisPoolConfig.setTestWhileIdle(testWhileIdle);

			jedisPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
			jedisPoolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
			jedisPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);

			return new JedisPool(jedisPoolConfig, host, port, timeBetweenEvictionRunsMillis, null);
			
		}
	}
}
