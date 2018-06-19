package com.feeyo.net.codec.http;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.UnknowProtocolException;
import com.feeyo.net.nio.NIOHandler;
import com.feeyo.net.nio.util.StringUtil;
import com.google.common.primitives.Bytes;

public class ProtobufFrontConnectionHandler implements NIOHandler<ProtobufFrontConnection>{
	
	private static final Charset charset = Charset.forName("utf-8"); 
	private static final Logger LOGGER = LoggerFactory.getLogger( ProtobufFrontConnectionHandler.class ); 
	private Decoder<List<ProtobufRequest>> decoder = null;
	
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
		
		if(data == null || data.length < 4)	//TODO 待处理data.length < 4 的情况
			return;
		
		List<ProtobufRequest> reqList = null;
		try {
			if(Bytes.indexOf(data, "HTTP".getBytes(charset)) != -1) {		//判断是http还是tcp请求
				if(Bytes.indexOf(data, "POST".getBytes(charset)) != -1) {	//判断是否是post请求
					decoder = new HttpDecoder();
					reqList = decoder.decode(data);
				}
			}else {
				decoder = new ProtobufRequestDecoder();
				reqList = decoder.decode(data);
			}
			System.out.println(reqList);	
			conn.write( "OK".getBytes() );
		} catch (UnknowProtocolException e) {
			LOGGER.error(e.getMessage());
			conn.write( "ERROR".getBytes());
		}
		
	}
	
}
