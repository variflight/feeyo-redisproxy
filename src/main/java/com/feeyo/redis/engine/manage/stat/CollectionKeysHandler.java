package com.feeyo.redis.engine.manage.stat;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.manage.stat.StatUtil.CollectionKey;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.backend.pool.cluster.ClusterCRC16Util;
import com.feeyo.redis.net.backend.pool.cluster.RedisClusterPool;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;
import com.feeyo.util.jedis.exception.JedisDataException;

public class CollectionKeysHandler {
	private static Logger LOGGER = LoggerFactory.getLogger( CollectionKeysHandler.class );
	
	private final static int COLLECTION_KEY_LENGTH_THRESHOLD = 10000;
	
	public void Handle(ConcurrentHashMap<String, CollectionKey> collectionKeys) {
		for (Entry<String, CollectionKey> entry : collectionKeys.entrySet()) {
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
					
					byte type = collectionKey.type;
					if (type == CommandParse.TYPE_HASH_CMD) {
						conn.sendCommand( RedisCommand.HLEN, collectionKey.key );
					} else if (type == CommandParse.TYPE_LIST_CMD) {
						conn.sendCommand( RedisCommand.LLEN, collectionKey.key );
					} else if (type == CommandParse.TYPE_SET_CMD) {
						conn.sendCommand( RedisCommand.SCARD, collectionKey.key );
					} else if (type == CommandParse.TYPE_SORTEDSET_CMD) {
						conn.sendCommand( RedisCommand.ZCARD, collectionKey.key );
					}
					try {
						long length = conn.getIntegerReply();
						if ( length > COLLECTION_KEY_LENGTH_THRESHOLD ) {
							collectionKey.length.set((int) length);
							StatUtil.addCollectionKeyToTop100(collectionKey);
						} else {
							StatUtil.removeCollectionKeyFromTop100(collectionKey);
						}
					} catch (JedisDataException e1) {
					}
				} catch (JedisConnectionException e) {
					LOGGER.error("", e);	
				} finally {
					if ( conn != null ) {
						conn.disconnect();
					}
				}
				
			}
		}
	}
	
}
