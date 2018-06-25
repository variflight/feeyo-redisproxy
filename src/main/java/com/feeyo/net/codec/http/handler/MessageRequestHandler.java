package com.feeyo.net.codec.http.handler;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.feeyo.net.nio.ClosableConnection;
import com.google.protobuf.MessageLite;

public class MessageRequestHandler implements RequestHandler{

	private ProtobufDecoder decoder = null;

	@Override
	public void handle(ClosableConnection conn, String uri, byte[] data) {
		
		Map<String,String> paramMap = parseParamters(uri);
		
        boolean isCustomPkg = false;
        if(paramMap != null) {
        	String value = paramMap.get("isCustomPkg");
        	if(value != null) {
        		isCustomPkg = Boolean.valueOf(value);
        	} else {
        		conn.write("Error Req Param, Expected Param - isCustomPkg".getBytes());
        		return;
        	}
        }
		
		if(decoder == null || isCustomPkg != decoder.isCustomPkg()) {
			decoder = new ProtobufDecoder(Message.getDefaultInstance(), isCustomPkg);
		}
		
		List<MessageLite> msg = decoder.decode(data);
		
		if(msg == null) {
			conn.write("ERROR".getBytes());
		}else {
			conn.write("OK".getBytes());
		}
	}
	
	private Map<String, String> parseParamters(String uri) {

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
