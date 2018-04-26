package com.feeyo.redis.net.backend.pool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.BackendCallback;
import com.feeyo.redis.nio.util.TimeUtil;

/**
 * check for redis connections
 * 
 * @author zhuam
 *
 */
public class ConHeartBeatHandler implements BackendCallback {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ConHeartBeatHandler.class );
	
	private final ConcurrentHashMap<Long, HeartbeatCon> allCons = new ConcurrentHashMap<Long, HeartbeatCon>();

	public void doHeartBeat(RedisBackendConnection conn, byte[] buff) {
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("do heartbeat check for con " + conn);
		}

		try {
			HeartbeatCon hbCon = new HeartbeatCon(conn);
			boolean notExist = (allCons.putIfAbsent(hbCon.conn.getId(), hbCon) == null);
			if (notExist) {				
				conn.setCallback( this );
				conn.write( buff );
			}
		} catch (Exception e) {
			executeException(conn, e);
		}
	}
	
	public void doHeartBeat(RedisBackendConnection conn, ByteBuffer buff) {
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("do heartbeat check for con " + conn);
		}
		
		try {
			HeartbeatCon hbCon = new HeartbeatCon(conn);
			boolean notExist = (allCons.putIfAbsent(hbCon.conn.getId(), hbCon) == null);
			if (notExist) {				
				conn.setCallback( this );
				conn.write( buff );
			}
		} catch (Exception e) {
			executeException(conn, e);
		}
	}
	
	/**
	 * remove timeout connections
	 */
	public void abandTimeoutConns() {
		
		if (allCons.isEmpty()) {
			return;
		}
		
		Collection<RedisBackendConnection> abandCons = new LinkedList<RedisBackendConnection>();
		long curTime = System.currentTimeMillis();
		Iterator<Entry<Long, HeartbeatCon>> itors = allCons.entrySet().iterator();
		while (itors.hasNext()) {
			HeartbeatCon chkCon = itors.next().getValue();
			if (chkCon.timeoutTimestamp < curTime) {
				abandCons.add(chkCon.conn);
				itors.remove();
			}
		}

		if (!abandCons.isEmpty()) {
			for (RedisBackendConnection con : abandCons) {
				try {
					// if(con.isBorrowed())
					con.close("heartbeat check, backend conn is timeout !!! ");
				} catch (Exception e) {
					LOGGER.error("close err:", e);
				}
			}
		}
		abandCons.clear();

	}
	
	private void removeFinished(RedisBackendConnection con) {
		Long id = ((RedisBackendConnection) con).getId();
		this.allCons.remove(id);
	}
	
	private void executeException(RedisBackendConnection c, Throwable e) {
		removeFinished(c);
		LOGGER.error("executeException: ", e);
		c.close("heatbeat exception:" + e);
	}

	@Override
	public void connectionError(Exception e, RedisBackendConnection conn) {
		// not called
	}

	@Override
	public void connectionAcquired(RedisBackendConnection conn) {
		// not called
	}

	@Override
	public void handleResponse(RedisBackendConnection conn, byte[] byteBuff)
			throws IOException {
		
		removeFinished(conn);
		
		// +PONG\r\n
		if ( byteBuff.length == 7 &&  byteBuff[0] == '+' &&  byteBuff[1] == 'P' &&  byteBuff[2] == 'O' &&  byteBuff[3] == 'N' &&  byteBuff[4] == 'G'  ) {
			conn.setHeartbeatTime( TimeUtil.currentTimeMillis() );
			conn.release();	
			
		// kafka heartbeat
		} else if (byteBuff.length >= 4 && isOk(byteBuff)) {
			conn.setHeartbeatTime( TimeUtil.currentTimeMillis() );
			conn.release();		
			
		} else {
			conn.close("heartbeat err");
		}
	}

	private boolean isOk(byte[] buffer) {
		int len = buffer.length;
		if (len < 4) {
			return false;
		}
		int v0 = (buffer[0] & 0xff) << 24;
		int v1 = (buffer[1] & 0xff) << 16;  
		int v2 = (buffer[2] & 0xff) << 8;  
	    int v3 = (buffer[3] & 0xff); 
	    
	    if (v0 + v1 + v2 + v3 > len - 4) {
	    		return false;
	    }
		
		return true;
	}
	
	@Override
	public void connectionClose(RedisBackendConnection conn, String reason) {
		removeFinished(conn);		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug("connection closed " + conn + " reason:" + reason);
		}
	}
}


class HeartbeatCon {
	
	public final long timeoutTimestamp;
	public final RedisBackendConnection conn;

	public HeartbeatCon(RedisBackendConnection conn) {
		super();
		this.timeoutTimestamp = System.currentTimeMillis() + ( 20 * 1000L );
		this.conn = conn;
	}
}
