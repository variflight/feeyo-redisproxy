package com.feeyo.redis.net.backend.callback;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.nio.NetSystem;

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
	public void handleResponse(RedisBackendConnection backendCon, ByteBuffer byteBuff) throws IOException {

		try {
			if ( byteBuff.position() == 5 &&  byteBuff.get(0) == '+' &&  byteBuff.get(1) == 'O' &&  byteBuff.get(2) == 'K'  ) {
				
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
				byte[] b = new byte[byteBuff.position()];
				byteBuff.position(0);
				byteBuff.get(b, 0, b.length);
				backendCon.close("select database error:" + new String( b ) );
			}
		} finally {
			NetSystem.getInstance().getBufferPool().recycle(byteBuff);
		}
	}
	
}