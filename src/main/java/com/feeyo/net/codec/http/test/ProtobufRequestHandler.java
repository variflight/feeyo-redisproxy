package com.feeyo.net.codec.http.test;

import java.util.List;

import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.google.protobuf.MessageLite;

public class ProtobufRequestHandler implements RequestHandler{

	// 定义 protobuf prototype
	private ProtobufDecoder decoder = new ProtobufDecoder(Message.getDefaultInstance(), false);

	@Override
	public void handle(HttpConnection conn, String uri, byte[] data) {
		
		List<MessageLite> msg = decoder.decode(data);
		
		if(msg == null) {
			conn.write("ERROR".getBytes());
		}else {
			conn.write("OK".getBytes());
		}
	}

}
