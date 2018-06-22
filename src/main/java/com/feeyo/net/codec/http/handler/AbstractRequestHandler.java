package com.feeyo.net.codec.http.handler;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractRequestHandler implements RequestHandler {

	protected Map<String, String> parseParamters(String uri) {

		Map<String, String> parameters = new HashMap<String, String>();
		if (uri.indexOf("?") != -1)
			uri = uri.substring(uri.indexOf("?") + 1);
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
		return parameters;
	}

}
