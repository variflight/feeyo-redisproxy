package com.feeyo.protobuf.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

//
public class PBEncoder implements Encoder {
	
	@Override
	public <T> byte[] encode(T msg) throws InvalidProtocolBufferException {
		
		if (msg instanceof MessageLite) {
	            return ((MessageLite) msg).toByteArray();
        }
        if (msg instanceof MessageLite.Builder) {
            return ((MessageLite.Builder) msg).build().toByteArray();
        }
        
        throw new InvalidProtocolBufferException(msg.getClass().getName());
	}
	
}
