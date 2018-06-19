package com.feeyo.net.codec.http;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.NIOHandler;
import com.feeyo.net.nio.util.StringUtil;

public class HttpFrontConnectionHandler implements NIOHandler<HttpFrontConnection>{

	private static final Logger LOGGER = LoggerFactory.getLogger( HttpFrontConnectionHandler.class ); 
	
	@Override
	public void onConnected(HttpFrontConnection conn) throws IOException {
		LOGGER.info("onConnected(): {}", conn);
	}

	@Override
	public void onConnectFailed(HttpFrontConnection conn, Exception e) {
		LOGGER.info("onConnectFailed(): {}", conn);
	}

	@Override
	public void onClosed(HttpFrontConnection conn, String reason) {
		LOGGER.info("onClosed(): {}, {}", conn, reason);
	}

	@Override
	public void handleReadEvent(HttpFrontConnection conn, byte[] data) throws IOException {
		
		
		final String hexs = StringUtil.dumpAsHex(data, 0, data.length);
		LOGGER.info("C#{} front request len = {}, buffer bytes\n {}", 
				new Object[]{ conn.getId(), data.length, hexs });
		
		
		HttpDecoder decoder = new HttpDecoder();
		List<HttpMsgRequest> reqList = decoder.decode(data);
		System.out.println(reqList);
		conn.write( "OK".getBytes() );
		
	}
	
}
