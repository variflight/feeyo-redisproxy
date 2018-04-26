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

import com.feeyo.redis.net.backend.BackendConnection;
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
	
	protected final ConcurrentHashMap<Long, HeartbeatCon> allCons = new ConcurrentHashMap<Long, HeartbeatCon>();

	public void doHeartBeat(BackendConnection conn, byte[] buff) {
		
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
	
	public void doHeartBeat(BackendConnection conn, ByteBuffer buff) {
		
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
		
		Collection<BackendConnection> abandCons = new LinkedList<BackendConnection>();
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
			for (BackendConnection con : abandCons) {
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
	
	protected void removeFinished(BackendConnection con) {
		Long id = ((RedisBackendConnection) con).getId();
		this.allCons.remove(id);
	}
	
	protected void executeException(BackendConnection c, Throwable e) {
		removeFinished(c);
		LOGGER.error("executeException: ", e);
		c.close("heatbeat exception:" + e);
	}

	@Override
	public void connectionError(Exception e, BackendConnection conn) {
		// not called
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		// not called
	}

	@Override
	public void handleResponse(BackendConnection conn, byte[] byteBuff)
			throws IOException {
		
		removeFinished(conn);
		
		// +PONG\r\n
		if ( byteBuff.length == 7 &&  byteBuff[0] == '+' &&  byteBuff[1] == 'P' &&  byteBuff[2] == 'O' &&  byteBuff[3] == 'N' &&  byteBuff[4] == 'G'  ) {
			conn.setHeartbeatTime( TimeUtil.currentTimeMillis() );
			conn.release();	

		} else {
			conn.close("heartbeat err");
		}
	}
	
	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		removeFinished(conn);		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug("connection closed " + conn + " reason:" + reason);
		}
	}

}


class HeartbeatCon {
	
	public final long timeoutTimestamp;
	public final BackendConnection conn;

	public HeartbeatCon(BackendConnection conn) {
		super();
		this.timeoutTimestamp = System.currentTimeMillis() + ( 20 * 1000L );
		this.conn = conn;
	}
}
