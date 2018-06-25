package com.feeyo.net.codec.http.test;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class HttpRequestHandlerMagr {

	private static final String HTTP_METHOD_GET = "GET";
	private static final String HTTP_METHOD_POST = "POST";
	
	private static HttpRequestHandlerMagr _INSTANCE = null;
	
	// REST
	private final PathTrie<RequestHandler> getHandlers = new PathTrie<RequestHandler>();
	private final PathTrie<RequestHandler> postHandlers = new PathTrie<RequestHandler>();

	public static HttpRequestHandlerMagr INSTANCE() {
		if (_INSTANCE == null) {
			synchronized (HttpRequestHandlerMagr.class) {
				if (_INSTANCE == null) {
					_INSTANCE = new HttpRequestHandlerMagr();
				}
			}
		}
		return _INSTANCE;
	}

	private HttpRequestHandlerMagr() {
		
		registerHandler(HTTP_METHOD_POST, "/proto/eraftpb/message", new MessageRequestHandler());
	}

	public RequestHandler lookup(String method, String uri) {

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

	private void registerHandler(String method, String path, RequestHandler handler) {
		if (method.equals(HTTP_METHOD_GET)) {
			getHandlers.insert(path, handler);

		} else if (method.equalsIgnoreCase(HTTP_METHOD_POST)) {
			postHandlers.insert(path, handler);
		} else {
			throw new RuntimeException("HttpMethod is not supported");
		}
	}

}
