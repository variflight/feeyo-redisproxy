package com.feeyo.kafka.net.backend.pool;

import java.io.IOException;

import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.pool.ConHeartBeatHandler;
import com.feeyo.redis.nio.util.TimeUtil;

public class KafkaConHeartBeatHandler extends ConHeartBeatHandler {
	
	@Override
	public void handleResponse(BackendConnection conn, byte[] byteBuff)
			throws IOException {
		
		removeFinished(conn);

		// kafka heartbeat
		if (byteBuff.length >= 4 && isOk(byteBuff)) {
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
	

}
