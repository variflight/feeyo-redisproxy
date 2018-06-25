package com.feeyo.net.codec.protobuf.test;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.http.HttpParser;
import com.feeyo.net.nio.NIOHandler;
import com.feeyo.net.nio.util.StringUtil;

public class ProtobufFrontConnectionHandler implements NIOHandler<ProtobufFrontConnection>{
	
	private static final Logger LOGGER = LoggerFactory.getLogger( ProtobufFrontConnectionHandler.class ); 
	
	private HttpParser parser;
	
	public ProtobufFrontConnectionHandler() {
		this.parser = new HttpParser();
	}
	
	
	@Override
	public void onConnected(ProtobufFrontConnection conn) throws IOException {
		LOGGER.info("onConnected(): {}", conn);
	}

	@Override
	public void onConnectFailed(ProtobufFrontConnection conn, Exception e) {
		LOGGER.info("onConnectFailed(): {}", conn);
	}

	@Override
	public void onClosed(ProtobufFrontConnection conn, String reason) {
		LOGGER.info("onClosed(): {}, {}", conn, reason);
	}

	@Override
	public void handleReadEvent(ProtobufFrontConnection conn, byte[] data) throws IOException {
					
		final String hexs = StringUtil.dumpAsHex(data, 0, data.length);
		LOGGER.info("C#{} front request len = {}, buffer bytes\n {}", 
				new Object[]{ conn.getId(), data.length, hexs });
		
		parser.parse(conn, data);
	}
	
}
