package com.feeyo.net.codec.http.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

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
	
	// REST 
	//
	public static final String HTTP_METHOD_GET = "GET";
	public static final String HTTP_METHOD_POST = "POST";
	
	private final PathTrie<RequestHandler> getHandlers = new PathTrie<RequestHandler>();
	private final PathTrie<RequestHandler> postHandlers = new PathTrie<RequestHandler>();
	
	public void registerHandler(String method, String path, RequestHandler handler) {
		if (method.equals(HTTP_METHOD_GET)) {
			getHandlers.insert(path, handler);

		} else if (method.equalsIgnoreCase(HTTP_METHOD_POST)) {
			postHandlers.insert(path, handler);
		} else {
			throw new RuntimeException("HttpMethod is not supported");
		}
	}
	
	
	private RequestHandler getHandler(String method, String uri) {

		RequestHandler handler = null;

		// 获取Path
		String path = null;
		int pathEndPos = uri.indexOf('?');
		if (pathEndPos < 0) {
			path = uri;
		} else {
			path = uri.substring(0, pathEndPos);
		}

		// 获取参数
		Map<String, String> parameters = new HashMap<String, String>();
		if (uri.startsWith("?")) {
			uri = uri.substring(1, uri.length());
		}
		String[] querys = uri.split("&");
		for (String query : querys) {
			String[] pair = query.split("=");
			if (pair.length == 2) {
				try {
					parameters.put(URLDecoder.decode(pair[0], "UTF8"), URLDecoder.decode(pair[1], "UTF8"));
				} catch (UnsupportedEncodingException e) {
					parameters.put(pair[0], pair[1]);
				}
			}
		}

		if (method.equals(HTTP_METHOD_GET)) {
			handler = getHandlers.retrieve(path, parameters);

		} else if (method.equals(HTTP_METHOD_POST)) {
			handler = postHandlers.retrieve(path, parameters);
		}
		return handler;
	}

	
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
				
				String method = request.getMethod();
				String uri = request.getUri();
				
				// 处理 path & protobuf 对于转换
				//
				RequestHandler requestHandler = getHandler( method, uri );
				requestHandler.handle(conn, uri, data);
				
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
