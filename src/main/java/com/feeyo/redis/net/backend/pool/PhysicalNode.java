package com.feeyo.redis.net.backend.pool;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.RedisBackendConnectionFactory;
import com.feeyo.redis.net.backend.callback.BackendCallback;
import com.feeyo.redis.nio.util.TimeUtil;


/**
 * Redis 物理节点的连接池
 * 
 * @author zhuam
 *
 */
public class PhysicalNode {
	
	private static Logger LOGGER = LoggerFactory.getLogger( PhysicalNode.class );

	protected final String name;
	protected final int size;
	
	// TODO: 待优化
	public final ConQueue conQueue = new ConQueue();
	
	protected final int poolType;
	protected final String poolName;
	
	protected String host;
	protected int port;
	protected int minCon;
	protected int maxCon;
	
	protected final RedisBackendConnectionFactory factory;
	
	public PhysicalNode(RedisBackendConnectionFactory factory, int poolType, String poolName, 
			int minCon, int maxCon, String host, int port) {
		
		this.factory = factory;
		this.poolType = poolType;
		this.poolName = poolName;
		
		this.host = host;
		this.port = port;
		this.minCon = minCon;
		this.maxCon = maxCon;
		
		this.size = maxCon;
		this.name = host + ":" + port;		
	}
	
	// 新建连接，异步填充后端连接池
	public void createNewConnection() throws IOException {	
		
		createNewConnection(new BackendCallback() {
			@Override
			public void connectionAcquired(RedisBackendConnection conn) {
				conQueue.getCons().add( conn ); 
			}	
			
			public void connectionClose(RedisBackendConnection conn, String reason) {
				conQueue.getCons().remove( conn );  
			}

			@Override
			public void handleResponse(RedisBackendConnection conn, byte[] byteBuff) throws IOException {
				//ignore
			}

			@Override
			public void connectionError(Exception e, RedisBackendConnection conn) {
				//ignore
			}

		}, null);
	}
	
	public RedisBackendConnection createNewConnection(BackendCallback callback, Object attachment) throws IOException {
		
		 int activeCons = this.getActiveCount();// 当前最大活动连接
         if ( activeCons + 1 > size ) {// 下一个连接大于最大连接数
         		LOGGER.error("PN={} the max activeConns={} size can not be max than maxConns={}", new Object[]{ name, (activeCons+1), size } );
             	throw new IOException("the max activeConnnections size can not be max than maxconnections");                
         } else {      
         	
         	if ( LOGGER.isDebugEnabled() ) 
         		LOGGER.debug( " no ilde connection in pool, create new connection for " + this.name + " of " + poolName);           
             
         	// create connection
        	RedisBackendConnection con = factory.make(this, callback, attachment);
    		con.setLastTime( TimeUtil.currentTimeMillis() );
    		return con;
         }
	}
	
	public boolean initConnections() {
		
        int initSize = minCon;
        LOGGER.info("init redis node ,create connections total " + initSize + " for " + host + ":" + port);
		for (int i = 0; i < initSize; i++) {
			try {
				this.createNewConnection();				
			} catch (Exception e) {
				LOGGER.error(" init connection error.", e);
			}
		}
		
		LOGGER.info("init redis node finished");
        return true;
    }
	
	
	public int getActiveCount() {
        return this.conQueue.getActiveCountForNode(this);
    }
	
	public int getIdleCount() {
		return this.conQueue.getCons().size();
	}

    public void clearConnections(String reason, boolean isForce) {    	
    	LOGGER.info("{} node cleanup, reason={}, force={}", new Object[] { this.name, reason, isForce });    	
    	if ( isForce ) {
    		this.conQueue.clearConnections(reason, this);
    		
    	} else {
        	long idleTimeout = 1000 * 45;  //45s 超时 
    		this.conQueue.setIdleTimeConnections(this, idleTimeout);
    	}
    }

    public RedisBackendConnection getConnection(BackendCallback callback, Object attachment)
            throws IOException {
    	
    	RedisBackendConnection con = this.conQueue.takeIdleCon();
        if (con != null) {
        	con.setAttachement( attachment );
        	con.setCallback( callback );        	
        	con.setBorrowed(true);
        	con.setLastTime( TimeUtil.currentTimeMillis() ); // 每次取连接的时候，更新下lasttime，防止在前端连接检查的时候，关闭连接，导致执行失败
        	return con;
        } 
        
        return null;
    }
	
	public void releaseConnection(RedisBackendConnection c) {
        
		c.setBorrowed( false );
        c.setAttachement( null );
        c.setLastTime( TimeUtil.currentTimeMillis() );     
        
        ConQueue queue = this.conQueue;
        boolean ok = false;
        ok = queue.getCons().offer(c);
        if ( !ok ) {
        	LOGGER.warn("can't return to pool ,so close con " + c);
            c.close("can't return to pool ");
        }
        
        if ( LOGGER.isDebugEnabled() ) {
        	LOGGER.debug("release channel " + c);
        }
    }
	
	public void removeConnection(RedisBackendConnection conn) {
		
		ConQueue queue = this.conQueue;
		if (queue != null) {
			queue.removeCon(conn);
		}
	}	
	
	public String getName() {
		return name;
	}

	public int getPoolType() {
		return poolType;
	}

	public String getPoolName() {
		return poolName;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public int getMinCon() {
		return minCon;
	}

	public int getMaxCon() {
		return maxCon;
	}	
	
	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}		
	
}