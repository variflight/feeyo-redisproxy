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
	public void handleResponse(RedisBackendConnection source, byte[] byteBuff) throws IOException {

		if ( byteBuff.length == 5 &&  byteBuff[0] == '+' &&  byteBuff[1] == 'O' &&  byteBuff[2] == 'K'  ) {
			
			if ( nextCallback != null ) {
				// set database
				source.setDb( db );
				
				// reset callback
				source.setCallback( nextCallback );	
				
				// write real packet
				if ( nextCmd != null )
					source.write( nextCmd );
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
	
}