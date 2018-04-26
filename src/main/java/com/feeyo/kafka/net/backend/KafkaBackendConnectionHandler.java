package com.feeyo.kafka.net.backend;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.NIOHandler;
import com.feeyo.redis.nio.util.StringUtil;

public class KafkaBackendConnectionHandler implements NIOHandler<KafkaBackendConnection> {

	private static Logger LOGGER = LoggerFactory.getLogger( KafkaBackendConnectionHandler.class );
	
	@Override
	public void onConnected(KafkaBackendConnection con) throws IOException {	
        // 已经连接成功
		con.getCallback().connectionAcquired( con );		
	}
	
	@Override
	public void handleReadEvent(KafkaBackendConnection con, byte[] data) throws IOException {
		
		// 日志HEX
		if ( LOGGER.isDebugEnabled() ) {
			final String hexs = StringUtil.dumpAsHex(data, 0, data.length);
			LOGGER.debug("C#{} backend response len = {},  buffer bytes\n {}", 
					new Object[]{ con.getId(), data.length, hexs });
		}
		
		
		// 回调
		con.getCallback().handleResponse(con, data);	
		return;	
	}	

	@Override
	public void onClosed(KafkaBackendConnection con, String reason) {
		if ( con.getCallback() != null )
			con.getCallback().connectionClose(con, reason);
	}

	@Override
	public void onConnectFailed(KafkaBackendConnection con, Exception e) {
		if ( con.getCallback() != null )
			con.getCallback().connectionError(e, con);		
	}	

}