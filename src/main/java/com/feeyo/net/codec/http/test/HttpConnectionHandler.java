package com.feeyo.net.codec.http.test;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.UnknowProtocolException;
import com.feeyo.net.codec.http.HttpDecoder;
import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.net.nio.NIOHandler;
import com.feeyo.net.nio.util.StringUtil;

public class HttpConnectionHandler implements NIOHandler<HttpConnection>{
	
	private static final Logger LOGGER = LoggerFactory.getLogger( HttpConnectionHandler.class ); 
	
	private HttpDecoder decoder = new HttpDecoder();
	
	
	@Override
	public void onConnected(HttpConnection conn) throws IOException {
		LOGGER.info("onConnected(): {}", conn);
	}

	@Override
	public void onConnectFailed(HttpConnection conn, Exception e) {
		LOGGER.info("onConnectFailed(): {}", conn);
	}

	@Override
	public void onClosed(HttpConnection conn, String reason) {
		LOGGER.info("onClosed(): {}, {}", conn, reason);
	}

	@Override
	public void handleReadEvent(HttpConnection conn, byte[] data) throws IOException {
					
		final String hexs = StringUtil.dumpAsHex(data, 0, data.length);
		LOGGER.info("C#{} front request len = {}, buffer bytes\n {}", 
				new Object[]{ conn.getId(), data.length, hexs });
		
		try {
			HttpRequest request = decoder.decode(data );
			if ( request != null ) {
				// 处理 path & protobuf 对于转换
				//
				
				
			}
			
		} catch (UnknowProtocolException e) {
			e.printStackTrace();
		}
		
	}


	@Override
	public boolean handleNetFlow(HttpConnection con, int dataLength) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}
	
}
