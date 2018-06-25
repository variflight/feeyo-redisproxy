package com.feeyo.net.codec.http.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

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
	
	
	//
	//
	@SuppressWarnings({"unused","unchecked","rawtypes"})
	public static class TrieNode<T> {
		private transient String key;
		private transient T value;
		private boolean isWildcard;
		private final String wildcard;

		private transient String namedWildcard;

		private Map<String, TrieNode<T>> children;

		private final TrieNode<T> parent;

		public TrieNode(String key, T value, TrieNode<T> parent, String wildcard) {
			this.key = key;
			this.wildcard = wildcard;
			this.isWildcard = (key.equals(wildcard));
			this.parent = parent;
			this.value = value;
			this.children = Collections.emptyMap();
			if (isNamedWildcard(key)) {
				namedWildcard = key.substring(key.indexOf('{') + 1,
						key.indexOf('}'));
			} else {
				namedWildcard = null;
			}
		}

		public void updateKeyWithNamedWildcard(String key) {
			this.key = key;
			namedWildcard = key.substring(key.indexOf('{') + 1,
					key.indexOf('}'));
		}

		public boolean isWildcard() {
			return isWildcard;
		}

		public synchronized void addChild(TrieNode<T> child) {
			Map m = new ConcurrentHashMap(children);
			m.put(child.key, child);
			children = Collections.unmodifiableMap(m);

		}

		public TrieNode getChild(String key) {
			return children.get(key);
		}

		public synchronized void insert(String[] path, int index, T value) {
			if (index >= path.length)
				return;

			String token = path[index];
			String key = token;
			if (isNamedWildcard(token)) {
				key = wildcard;
			}
			TrieNode<T> node = children.get(key);
			if (node == null) {
				if (index == (path.length - 1)) {
					node = new TrieNode<T>(token, value, this, wildcard);
				} else {
					node = new TrieNode<T>(token, null, this, wildcard);
				}
				Map m = new ConcurrentHashMap(children);
				m.put(key, node);
				children = Collections.unmodifiableMap(m);

			} else {
				if (isNamedWildcard(token)) {
					node.updateKeyWithNamedWildcard(token);
				}

				// in case the target(last) node already exist but without a
				// value
				// than the value should be updated.
				if (index == (path.length - 1)) {
					assert (node.value == null || node.value == value);
					if (node.value == null) {
						node.value = value;
					}
				}
			}

			node.insert(path, index + 1, value);
		}

		private boolean isNamedWildcard(String key) {
			return key.indexOf('{') != -1 && key.indexOf('}') != -1;
		}

		private String namedWildcard() {
			return namedWildcard;
		}

		private boolean isNamedWildcard() {
			return namedWildcard != null;
		}

		public T retrieve(String[] path, int index, Map<String, String> params) {
			if (index >= path.length)
				return null;

			String token = path[index];
			TrieNode<T> node = children.get(token);
			boolean usedWildcard = false;
			if (node == null) {
				node = children.get(wildcard);
				if (node == null) {
					return null;
				} else {
					usedWildcard = true;
					if (params != null && node.isNamedWildcard()) {
						params.put(node.namedWildcard(), token);
					}
				}
			}

			if (index == (path.length - 1)) {
				return node.value;
			}

			T res = node.retrieve(path, index + 1, params);
			if (res == null && !usedWildcard) {
				node = children.get(wildcard);
				if (node != null) {
					if (params != null && node.isNamedWildcard()) {
						params.put(node.namedWildcard(), token);
					}
					res = node.retrieve(path, index + 1, params);
				}
			}

			return res;
		}
	}
	
	//
	//
	public class PathTrie<T> {
		private final TrieNode<T> root;
		private final Pattern pattern;
		private T rootValue;

		public PathTrie() {
			this("/", "*");
		}

		public PathTrie(String separator, String wildcard) {
			pattern = Pattern.compile(separator);
			root = new TrieNode<T>(separator, null, null, wildcard);
		}

		

		public void insert(String path, T value) {
			String[] strings = pattern.split(path);
			if (strings.length == 0) {
				rootValue = value;
				return;
			}
			int index = 0;
			// supports initial delimiter.
			if (strings.length > 0 && strings[0].isEmpty()) {
				index = 1;
			}
			root.insert(strings, index, value);
		}

		public T retrieve(String path) {
			return retrieve(path, null);
		}

		public T retrieve(String path, Map<String, String> params) {
			if (path.length() == 0) {
				return rootValue;
			}
			String[] strings = pattern.split(path);
			if (strings.length == 0) {
				return rootValue;
			}
			int index = 0;
			// supports initial delimiter.
			if (strings.length > 0 && strings[0].isEmpty()) {
				index = 1;
			}
			return root.retrieve(strings, index, params);
		}
	}
	
	
	
}
