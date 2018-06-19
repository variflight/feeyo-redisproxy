package com.feeyo.net.codec.http;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;


public class HttpEncoder {
	
	private static final Charset charset = Charset.forName("utf-8"); 
	private static final byte[] boundaryPart = "====boundary-part====".getBytes(charset);
	
	private ProtobufRequestEncoder encoder;
	
	public HttpEncoder() {
		encoder = new ProtobufRequestEncoder();
	}
	
	public ByteBuffer encode(ProtobufRequest req) {
		
		if(req == null || req.getMsgList() == null || req.getMsgList().isEmpty())
			return null;
		
		
		ByteBuffer reqBuff = encoder.encode(req);
		if(reqBuff == null)
			return null;
		
		int reqSize = req.getNormalSize();
		ByteBuffer buffer  = ByteBuffer.allocate(reqSize + boundaryPart.length);
		
		buffer.put(boundaryPart);
		buffer.put(reqBuff);
		buffer.flip();
		return buffer;
	}

}
