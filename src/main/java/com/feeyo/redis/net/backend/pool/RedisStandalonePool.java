package com.feeyo.redis.net.backend.pool;

import java.util.LinkedList;

import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.RedisBackendConnectionFactory;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;

/**
 * 单节点, Redis 连接池
 * 
 * @author zhuam
 *
 */
public class RedisStandalonePool extends AbstractPool {
	
	private PhysicalNode physicalNode;

	public volatile int heartbeatRetry = 0;
	public volatile int heartbeatStatus = 1;	
	public volatile long heartbeatTime = -1;
	
	public RedisStandalonePool(PoolCfg poolCfg) {
		super( poolCfg );
	}

	@Override
	public boolean startup() {			
		int size = poolCfg.getNodes().size();		
		if ( size != 1 ) {
			LOGGER.error("startup err: size#{}", size);
			return false;
		}
		
		// 获取配置信息
		int poolType = poolCfg.getType();
		String poolName = poolCfg.getName();
		int minCon = poolCfg.getMinCon();
		int maxCon = poolCfg.getMaxCon();
		
		String[] ipAndPort = poolCfg.getNodes().get(0).split(":");
		final RedisBackendConnectionFactory bcFactory = RedisEngineCtx.INSTANCE().getBackendRedisConFactory();		
		this.physicalNode = new PhysicalNode(bcFactory, poolType, poolName, minCon, maxCon, ipAndPort[0], Integer.parseInt( ipAndPort[1] ) );
		this.physicalNode.initConnections();		
		return true;
	}
	
	@Override
	public boolean close(boolean isForce) {
		physicalNode.clearConnections("manual reload", isForce);
		return true;
	}
	
	// 当前可用的 Node（负载均衡模式）
	@Override
	public PhysicalNode getPhysicalNode() {		
		if ( heartbeatStatus == 1 ) {
			return this.physicalNode;
		}
		return null;
	}

	@Override
	public PhysicalNode getPhysicalNode(String cmd, String key) {
		throw new UnsupportedOperationException("Not implemented");
	}
	
	@Override
	public PhysicalNode getPhysicalNode(String cmd, byte[] key) {
		throw new UnsupportedOperationException("Not implemented");
	}
	
	/*
	 * 此处不依赖 physicalNode 的初始化， 直接进行连接探测
	 * @see com.feeyo.redis.net.backend.pool.AbstractPool#testConnection()
	 */
	@Override
	public boolean testConnection() {		
		boolean result = false;		
		int size = poolCfg.getNodes().size();		
		if ( size == 1 ) {
			String[] ipAndPort = poolCfg.getNodes().get(0).split(":");
			JedisConnection conn = null;		
			try {
				conn = new JedisConnection(ipAndPort[0], Integer.parseInt( ipAndPort[1] ), 2000, 0);
				conn.sendCommand( RedisCommand.PING );
				String value = conn.getBulkReply();
				if ( "PONG".equalsIgnoreCase( value ) ) {
					result = true;
				} else {
					LOGGER.error("test connection err: {}, {}", ipAndPort[0] + ":" + ipAndPort[1], value);
				}
			} catch (JedisConnectionException e) {
				LOGGER.error("test connection err: {}:{}", ipAndPort[0], ipAndPort[1]);
				result = false;
			} finally {
				if ( conn != null ) {
					conn.disconnect();
				}
			}
		}
		return result;
	}

	@Override
	public void availableCheck() {
		
		// 加把锁， 避免网络不好的情况下，频繁并发的检测
		// TODO: 因为是定时的检测，此处不做CAS 的自旋
		if ( !availableCheckFlag.compareAndSet(false,  true) ) {
			return;
		}

		JedisConnection conn = null;		
		try {
			
			String host = this.physicalNode.getHost();
			int port = this.physicalNode.getPort();	
			
			conn = new JedisConnection(host, port, 5000, 0);
			conn.sendCommand( RedisCommand.PING );
			String value = conn.getBulkReply();
			if ( "PONG".equalsIgnoreCase( value ) ) {
				heartbeatRetry = 0;
				heartbeatStatus  = 1;				
			} else {				
				heartbeatRetry++;
				if ( heartbeatRetry == 3 ) {
					heartbeatStatus = -1;
				}
			}
			
		} catch (JedisConnectionException e) {
			LOGGER.error("available check err:", e);	
			
			heartbeatRetry++;
			if ( heartbeatRetry == 3 ) {
				heartbeatStatus = -1;
			}
		} finally {
			
			availableCheckFlag.set( false );
			
			heartbeatTime = TimeUtil.currentTimeMillis();
			if ( conn != null ) {
				conn.disconnect();
			}
		}
		
		// 关闭之前连接
		if ( heartbeatStatus == -1 ) {
			physicalNode.clearConnections("this node exception, automatic reload", true);
		}
	}
	
	@Override
	public void heartbeatCheck(long timeout) {	
		
		// 心跳检测, 超时抛弃 
		// --------------------------------------------------------------------------
		long heartbeatTime = System.currentTimeMillis() - timeout;
		long closeTime = System.currentTimeMillis() - timeout * 2;
		
		LinkedList<RedisBackendConnection> heartBeatCons = getNeedHeartbeatCons( physicalNode.conQueue.getCons(), heartbeatTime, closeTime);			
		for (RedisBackendConnection conn : heartBeatCons) {
			conHeartBeatHanler.doHeartBeat(conn, PING );
		}
		heartBeatCons.clear();		
		conHeartBeatHanler.abandTimeoutConns();
		
		// 连接池 动态调整逻辑
		// -------------------------------------------------------------------------------
		int idleCons = this.getIdleCount();
		int activeCons = this.getActiveCount();
		int minCons = poolCfg.getMinCon();
		int maxCons = poolCfg.getMaxCon();
		
		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug( "Sthandalone heartbeat: host={}, idle={}, active={}, min={}, max={}, lasttime={}", 
					new Object[] { physicalNode.getHost() + ":" + physicalNode.getPort(),  
					idleCons, activeCons, minCons, maxCons, System.currentTimeMillis() } );
		}
		
		if ( idleCons > minCons ) {	
			
			if ( idleCons < activeCons ) {
				return;
			}		
			
			//闲置太多
			closeByIdleMany(this.physicalNode, idleCons - minCons );
			
		} else if ( idleCons < minCons ) {
			
			if ( idleCons > ( minCons * 0.5 ) ) {
				return;
			}
			
			//闲置太少
			if ( (idleCons + activeCons) < maxCons ) {	
				int createCount =  (int)Math.ceil( (minCons - idleCons) / 3F );			
				createByIdleLitte(this.physicalNode, idleCons, createCount);
			}			
		}
		
//		if ( ( (idleCons + activeCons) < maxCons ) && idleCons < minCons ) {			
//			//闲置太少
//			int createCount =  (int)Math.ceil( (minCons - idleCons) / 3F );		
//			createByIdleLitte(this.physicalNode, idleCons, createCount);
//		
//		} else if ( idleCons > minCons ) {
//			//闲置太多
//			closeByIdleMany(this.physicalNode, idleCons - minCons );			
//		}
	}
	
	public int getActiveCount() {
		return this.physicalNode.getActiveCount();
	}
	
	public int getIdleCount() {
		return this.physicalNode.getIdleCount();
	}

	@Override
	public String toString(){
		final StringBuilder sbuf = new StringBuilder("RedisPool[")
		.append("name=").append( poolCfg.getName() ).append(',')
		.append("nodes=").append('[');
		sbuf.append( physicalNode );
		sbuf.append(']')
		.append(']');
		return (sbuf.toString());
	}

	@Override
	public PhysicalNode getPhysicalNode(int id) {
		return null;
	}	
	
}