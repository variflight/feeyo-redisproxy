package com.feeyo.redis.net.backend.callback;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.RedisBackendConnection;

/**
 * 支持 select database 
 * 
 * @author zhuam
 *
 */
public class SelectDbCallback extends AbstractBackendCallback {
	
	protected BackendCallback nextCallback = null;		//
	protected ByteBuffer nextCmd = null;
	
	private int db;
	
	public SelectDbCallback(int db, BackendCallback nextCallback, ByteBuffer nextCmd) {
		this.db = db;
		this.nextCallback = nextCallback;
		this.nextCmd = nextCmd;
    }
	
	@Override
	public void handleResponse(BackendConnection conn, byte[] byteBuff) throws IOException {

		RedisBackendConnection backendCon = (RedisBackendConnection)conn;
		
		if ( byteBuff.length == 5 &&  byteBuff[0] == '+' &&  byteBuff[1] == 'O' &&  byteBuff[2] == 'K'  ) {
			
			if ( nextCallback != null ) {
				// set database
				backendCon.setDb( db );
				
				// reset callback
				backendCon.setCallback( nextCallback );	
				
				// write real packet
				if ( nextCmd != null )
					backendCon.write( nextCmd );
				return;
			}
			
		} else {
			backendCon.close("select database error:" + new String( byteBuff ) );
		}
	}
	
}