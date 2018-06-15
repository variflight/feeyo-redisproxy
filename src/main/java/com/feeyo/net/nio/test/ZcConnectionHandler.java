package com.feeyo.net.nio.test;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.NIOHandler;
import com.feeyo.net.nio.util.StringUtil;

public class ZcConnectionHandler implements NIOHandler<ZcConnection> {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ZcConnectionHandler.class );
	
	public static final byte[] OK =   "+OK\r\n".getBytes();

	@Override
	public void onConnected(ZcConnection conn) throws IOException {
		LOGGER.info("onConnected(): {}", conn);
	}

	@Override
	public void onConnectFailed(ZcConnection conn, Exception e) {
		LOGGER.info("onConnectFailed(): {}", conn);
	}

	@Override
	public void onClosed(ZcConnection conn, String reason) {
		LOGGER.info("onClosed(): {}, {}", conn, reason);
	}

	@Override
	public void handleReadEvent(ZcConnection conn, byte[] data) throws IOException {
		
		final String hexs = StringUtil.dumpAsHex(data, 0, data.length);
		LOGGER.info("C#{} front request len = {}, buffer bytes\n {}", 
				new Object[]{ conn.getId(), data.length, hexs });
		
		
		conn.write( OK );
	}
	

}
