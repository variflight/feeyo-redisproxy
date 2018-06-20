package com.feeyo.net.codec.http;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.Encoder;
import com.feeyo.net.codec.protobuf.ProtobufEncoder;
import com.google.protobuf.MessageLite;

public class ProtobufRequestEncoder implements Encoder<ProtobufRequest>{
	
	private static Logger LOGGER = LoggerFactory.getLogger(ProtobufRequestEncoder.class);
	
	
	@Override
	public ByteBuffer encode(ProtobufRequest req) {
		
		if(req == null || req.getMsgList() == null || req.getMsgList().isEmpty())
			return null;
		
		boolean isCustomPkg = req.isCustomPkg();
		int reqSize = req.getNormalSize();
		int protoType = req.getProtoType();
		
		ProtobufEncoder encoder = new ProtobufEncoder(isCustomPkg);
		ByteBuffer buffer  = ByteBuffer.allocate(reqSize);
		
		
		buffer.put( (byte) ((protoType >> 24) & 0xFF) );
		buffer.put( (byte) ((protoType >> 16) & 0xFF) );
		buffer.put( (byte) ((protoType >> 8) & 0xFF) );
		buffer.put( (byte) (protoType & 0xFF) );
		
		buffer.put( (byte) (isCustomPkg ? 0x00 : 0x01));
		
		for(MessageLite msg : req.getMsgList()) {
			ByteBuffer content = encoder.encode(msg);
			if(content == null) {
				LOGGER.error("msg empty or encode exception");
				return null;
			}
			buffer.put(content);
		}
		buffer.flip();
		return buffer;
	}

}