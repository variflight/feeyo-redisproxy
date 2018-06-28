package com.feeyo.net.codec.http.test;

import java.util.List;

import com.feeyo.net.codec.http.HttpResponse;
import com.feeyo.net.codec.http.HttpResponseEncoder;
import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.google.protobuf.MessageLite;

public class ProtobufRequestHandler implements RequestHandler{

	// 定义 protobuf prototype
	private ProtobufDecoder decoder = new ProtobufDecoder(Message.getDefaultInstance(), false);

	@Override
	public void handle(HttpConnection conn, String uri, byte[] data) {
		
		List<MessageLite> msg = decoder.decode(data);
		HttpResponse response = msg == null ? new HttpResponse(400, "PARSE MSG ERROR") : new HttpResponse(200, "OK");
		HttpResponseEncoder encoder = new HttpResponseEncoder();
		conn.write(encoder.encode(response));
		conn.close("finish");
	}

}
