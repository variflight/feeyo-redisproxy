package com.feeyo.redis.net.backend;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.NIOHandler;
import com.feeyo.redis.nio.util.StringUtil;

/**
 * backend redis NIO handler (only one for all backend redis connections)
 * 
 * @author zhuam
 *
 */
public class RedisBackendConnectionHandler implements NIOHandler<RedisBackendConnection> {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RedisBackendConnectionHandler.class );
	
	@Override
	public void onConnected(RedisBackendConnection con) throws IOException {	
        // 已经连接成功
		con.getCallback().connectionAcquired( con );		
	}
	
	@Override
	public void handleReadEvent(RedisBackendConnection con, ByteBuffer data) throws IOException {
		
		// 日志HEX
		if (LOGGER.isDebugEnabled()) {
			int length = data.position();
			byte[] b = new byte[length];
			data.get(b, 0, length);
			final String hexs = StringUtil.dumpAsHex(b, 0, b.length);
			LOGGER.debug("C#{} front request len = {}, buffer bytes\n {}",
					new Object[] { con.getId(), b.length, hexs });
		}
		
		// 回调
		con.getCallback().handleResponse(con, data);	
		return;	
	}	

	@Override
	public void onClosed(RedisBackendConnection con, String reason) {
		if ( con.getCallback() != null )
			con.getCallback().connectionClose(con, reason);
	}

	@Override
	public void onConnectFailed(RedisBackendConnection con, Exception e) {
		if ( con.getCallback() != null )
			con.getCallback().connectionError(e, con);		
	}

}
