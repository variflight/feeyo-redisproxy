package com.feeyo.redis.nio.test;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.NIOHandler;
import com.feeyo.redis.nio.util.StringUtil;

public class ZeroCopyConnectionHandler implements NIOHandler<ZeroCopyConnection> {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ZeroCopyConnectionHandler.class );
	
	public static final byte[] OK =   "+OK\r\n".getBytes();

	@Override
	public void onConnected(ZeroCopyConnection conn) throws IOException {
		LOGGER.info("onConnected(): {}", conn);
	}

	@Override
	public void onConnectFailed(ZeroCopyConnection conn, Exception e) {
		LOGGER.info("onConnectFailed(): {}", conn);
	}

	@Override
	public void onClosed(ZeroCopyConnection conn, String reason) {
		LOGGER.info("onClosed(): {}, {}", conn, reason);
	}

	@Override
	public void handleReadEvent(ZeroCopyConnection conn, byte[] data) throws IOException {
		
		final String hexs = StringUtil.dumpAsHex(data, 0, data.length);
		LOGGER.info("C#{} front request len = {}, buffer bytes\n {}", 
				new Object[]{ conn.getId(), data.length, hexs });
		
		
		conn.write( OK );
	}
	

}
