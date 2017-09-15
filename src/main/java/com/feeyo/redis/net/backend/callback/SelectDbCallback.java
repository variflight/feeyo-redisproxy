package com.feeyo.redis.net.backend.callback;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;

/**
 * 支持 select database 
 * 
 * @author zhuam
 *
 */
public class SelectDbCallback implements BackendCallback {
	
	protected BackendCallback oldCallback = null;		//
	protected ByteBuffer buffer = null;
	
	private int db;
	
	public SelectDbCallback(int db, BackendCallback oldCallback, ByteBuffer buffer) {
		this.db = db;
		this.oldCallback = oldCallback;
		this.buffer = buffer;
    }
	
	// 获取后端连接
	protected RedisFrontConnection getFrontCon(RedisBackendConnection backendCon) {
		return (RedisFrontConnection) backendCon.getAttachement();
	}

	@Override
	public void handleResponse(RedisBackendConnection source, byte[] byteBuff) throws IOException {

		if ( byteBuff.length == 5 &&  byteBuff[0] == '+' &&  byteBuff[1] == 'O' &&  byteBuff[2] == 'K'  ) {
			
			if ( oldCallback != null ) {
				// set database
				source.setDb( db );
				
				// reset callback
				source.setCallback( oldCallback );	
				
				// write real packet
				if ( buffer != null )
					source.write( buffer );
				return;
			}
			
		} 
		
		// select database error
		RedisFrontConnection frontCon = getFrontCon(source);	
		if ( frontCon != null && !frontCon.isClosed() ) {
			frontCon.writeErrMessage("select database err:" + new String(byteBuff));
			return;
		}
	}

	@Override
	public void connectionAcquired(RedisBackendConnection conn) {
	}

	@Override
	public void connectionError(Exception e, RedisBackendConnection conn) {
		oldCallback.connectionError(e, conn);

	}

	@Override
	public void connectionClose(RedisBackendConnection conn, String reason) {
		oldCallback.connectionClose(conn, reason);
	}
	
}