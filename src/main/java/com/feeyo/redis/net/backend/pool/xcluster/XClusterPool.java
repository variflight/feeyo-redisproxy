package com.feeyo.redis.net.backend.pool.xcluster;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.feeyo.redis.config.PoolCfg;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.RedisBackendConnectionFactory;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.ConHeartBeatHandler;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisConnectionException;

/**
 * custom cluster pool
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class XClusterPool extends AbstractPool{
	
	protected ConHeartBeatHandler conHeartBeatHanler = new ConHeartBeatHandler();
	protected RedisBackendConnectionFactory backendConFactory = new RedisBackendConnectionFactory();
	
    private Map<String, XNode> nodes = new HashMap<>();

    public XClusterPool(PoolCfg poolCfg) {
        super(poolCfg);
    }

    @Override
    public boolean startup() {
    	
        int type = poolCfg.getType();
        String name = poolCfg.getName();
        int minCon = poolCfg.getMinCon();
        int maxCon = poolCfg.getMaxCon();
        
        for (String nodeStr : poolCfg.getNodes()) {
            
            String[] attrs = nodeStr.split(":");
            
            XNode xNode = new XNode();
            xNode.setIp( attrs[0] );
            xNode.setPort( Integer.parseInt(attrs[1]) );
            xNode.setSuffix( attrs[2] );

            PhysicalNode physicalNode = new PhysicalNode(backendConFactory, 
            		type, name, minCon, maxCon, xNode.getIp(), xNode.getPort());
            physicalNode.initConnections();
            xNode.setPhysicalNode(physicalNode);
            
            nodes.put(xNode.getSuffix(), xNode);
        }
        return false;
    }

    @Override
    public boolean close(boolean isForce) {
        return false;
    }

    @Override
    public PhysicalNode getPhysicalNode() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public PhysicalNode getPhysicalNode(String cmd, String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public PhysicalNode getPhysicalNode(String cmd, byte[] key) {
        return getPhysicalNode(cmd, new String(key));
    }

    @Override
    public boolean testConnection() {
        for (XNode xNode : nodes.values()) {
            PhysicalNode physicalNode = xNode.getPhysicalNode();
            String host = physicalNode.getHost();
            int port = physicalNode.getPort();

            JedisConnection conn = null;
            try {
                conn = new JedisConnection(host, port, 2000, 0);
                conn.sendCommand(RedisCommand.PING);
                String ret = conn.getBulkReply();
                if (!ret.toUpperCase().contains("PONG")) {
                    return false;
                }
            } catch (JedisConnectionException e) {
                LOGGER.error("test connection err: {}:{}", host, port);
                return false;
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
        }
        return true;
    }

    @Override
    public void availableCheck() {
    	
        if ( !availableCheckFlag.compareAndSet(false,  true) ) {
            return;
        }

        try {
            for (XNode node : nodes.values()) {
            	node.availableCheck();
            }
        } finally {
            availableCheckFlag.set( false );
        }
    }

    @Override
    public void heartbeatCheck(long timeout) {
    	
    	for(XNode node : nodes.values()) {
    		PhysicalNode physicalNode = node.getPhysicalNode();
    		heartbeatCheck(physicalNode, timeout);
    	}
    	
    }
    
	private void heartbeatCheck(PhysicalNode physicalNode, long timeout) {
		
		// 心跳检测, 超时抛弃 
		// --------------------------------------------------------------------------
		long heartbeatTime = System.currentTimeMillis() - timeout;
		long closeTime = System.currentTimeMillis() - timeout * 2;
		
		LinkedList<BackendConnection> heartBeatCons = getNeedHeartbeatCons( physicalNode.conQueue.getCons(), heartbeatTime, closeTime);			
		for (BackendConnection conn : heartBeatCons) {
			conHeartBeatHanler.doHeartBeat(conn, PING );
		}
		heartBeatCons.clear();		
		conHeartBeatHanler.abandTimeoutConns();
		
		// 连接池 动态调整逻辑
		// -------------------------------------------------------------------------------
		int idleCons = physicalNode.getIdleCount();
		int activeCons = physicalNode.getActiveCount();
		int minCons = poolCfg.getMinCon();
		int maxCons = poolCfg.getMaxCon();
		
		LOGGER.info( "XNode: host={}, idle={}, active={}, min={}, max={}, lasttime={}", 
				new Object[] { physicalNode.getHost() + ":" + physicalNode.getPort(),  
				idleCons, activeCons, minCons, maxCons, System.currentTimeMillis() } );
		
		if ( idleCons > minCons ) {	
			
			if ( idleCons < activeCons ) {
				return;
			}		
			
			//闲置太多
			closeByIdleMany(physicalNode, idleCons - minCons );
			
		} else if ( idleCons < minCons ) {
			
			if ( idleCons > ( minCons * 0.5 ) ) {
				return;
			}
			
			//闲置太少
			if ( (idleCons + activeCons) < maxCons ) {	
				int createCount =  (int)Math.ceil( (minCons - idleCons) / 3F );			
				createByIdleLitte(physicalNode, idleCons, createCount);
			}			
		}
		
	}
    
    public PhysicalNode getPhysicalNode(String suffix) {
    	PhysicalNode node = nodes.get( suffix ).getPhysicalNode();
        return node;
    }

	@Override
	public PhysicalNode getPhysicalNode(int id) {
		return null;
	}
	
}
