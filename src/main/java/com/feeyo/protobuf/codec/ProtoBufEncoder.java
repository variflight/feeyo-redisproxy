package com.feeyo.protobuf.codec;

import java.nio.ByteBuffer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

//
public class ProtoBufEncoder<T> extends Encoder<T> {
	
	
	@Override
	public byte[] encode(T msg) throws InvalidProtocolBufferException {
		
		if (msg instanceof MessageLite) {
	            return ((MessageLite) msg).toByteArray();
        }
        if (msg instanceof MessageLite.Builder) {
            return ((MessageLite.Builder) msg).build().toByteArray();
        }
        
        throw new InvalidProtocolBufferException(msg.getClass().getName());
	}
	
	public ByteBuffer encodeToByteBuffer(T msg) throws InvalidProtocolBufferException {
		return ByteBuffer.wrap(encode(msg));
	}

}
