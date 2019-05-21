package com.feeyo.util.jedis;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class JedisHolder {
	
	private static JedisHolder instance = new JedisHolder();
	
	private ConcurrentHashMap<String, JedisPool> pools = new ConcurrentHashMap<>();
	
	// 连接池中最大空闲的连接数
	private int maxIdle = 15;
	private int minIdle = 5;
	
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

	private int softMinEvictableIdleTimeMillis = 60 * 1000;
	private int timeBetweenEvictionRunsMillis = 30 * 1000;
	
	private JedisHolder() {}
	
	public static JedisHolder INSTANCE() {
		return instance;
	}
	
	public JedisPool getJedisPool(String ip, int port) {
		StringBuffer sb = new StringBuffer();
		sb.append(ip).append(":").append(port);
		String address = sb.toString();
		
		JedisPool jedisPool = pools.get(address);
		if ( jedisPool == null ) {
			synchronized (this) {
				jedisPool = pools.get(address);
				if ( jedisPool == null) {
					jedisPool = initialize(ip, port);
					pools.put(address, jedisPool);
				} 
			}
		}
		return jedisPool;
	}
	
	private JedisPool initialize(String host, int port) {

		GenericObjectPoolConfig jedisPoolConfig = new GenericObjectPoolConfig();
		jedisPoolConfig.setMaxIdle(maxIdle);
		jedisPoolConfig.setMinIdle(minIdle);
		jedisPoolConfig.setTestOnBorrow(testOnBorrow);
		jedisPoolConfig.setTestOnReturn(testOnReturn);
		jedisPoolConfig.setTestWhileIdle(testWhileIdle);

		jedisPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
		jedisPoolConfig.setMinEvictableIdleTimeMillis(-1);
		jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
		jedisPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);

		return new JedisPool(jedisPoolConfig, host, port, timeBetweenEvictionRunsMillis, null);
		
	}

}
