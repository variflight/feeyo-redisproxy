package com.feeyo.kafka.net.backend.broker.offset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.JedisHolder;
import com.feeyo.util.jedis.JedisPool;
import com.feeyo.util.jedis.RedisCommand;

public class OffsetRemoteAdmin {
	
	private static Logger LOGGER = LoggerFactory.getLogger( OffsetRemoteAdmin.class );
	
	// 获取offset
	public long getOffset(String ip, int port, String user, String topic, int partition) {
		long offset = -1;
		
		JedisPool jedisPool = JedisHolder.INSTANCE().getJedisPool(ip, port);
		JedisConnection conn = jedisPool.getResource();
		try {
			conn.sendCommand(RedisCommand.AUTH, user);
			conn.getStatusCodeReply();
			
			conn.sendCommand(RedisCommand.KGETOFFSET, topic, String.valueOf(partition));
			String str = conn.getStatusCodeReply();
			offset = Long.parseLong(str);
			
		} catch (Exception e) {
			LOGGER.error("get remote offset err:", e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		return offset;
	}
	
	// 返还 offset
	public String returnOffset(String ip, int port, String user, String topic, int partition, long offset) {
		
		JedisPool jedisPool = JedisHolder.INSTANCE().getJedisPool(ip, port);
		JedisConnection conn = jedisPool.getResource();
		try {
			conn.sendCommand(RedisCommand.AUTH, user);
			conn.getStatusCodeReply();
			
			conn.sendCommand(RedisCommand.KRETURNOFFSET, topic, String.valueOf(partition), String.valueOf(offset));
			return conn.getStatusCodeReply();
			
		} catch (Exception e) {
			LOGGER.error("return remote offset err:", e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		
		return null;
	}
	
}
