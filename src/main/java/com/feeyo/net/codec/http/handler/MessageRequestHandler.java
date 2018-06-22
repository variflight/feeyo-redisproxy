package com.feeyo.net.codec.http.handler;

import java.util.List;
import java.util.Map;

import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.feeyo.net.nio.ClosableConnection;
import com.google.protobuf.MessageLite;

public class MessageRequestHandler extends AbstractRequestHandler {

	private ProtobufDecoder decoder = null;

	@Override
	public void handle(ClosableConnection conn, String uri, byte[] data) {
		
		if(decoder == null) {
			
			Map<String,String> paramMap = parseParamters(uri);
	        boolean isCustomPkg = false;
	        if(paramMap != null) {
	        	String value = paramMap.get("isCustomPkg");
	        	if(value != null) {
	        		isCustomPkg = Boolean.valueOf(value);
	        	}
	        }
			decoder = new ProtobufDecoder(Message.getDefaultInstance(), isCustomPkg);
		}
		
		List<MessageLite> msg = decoder.decode(data);
		
		if(msg == null) {
			conn.write("Parse error".getBytes());
		}else {
			conn.write("Parse succ".getBytes());
		}
	}

}
