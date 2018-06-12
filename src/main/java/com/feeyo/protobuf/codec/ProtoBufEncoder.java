package com.feeyo.protobuf.codec;

import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

//
public class ProtoBufEncoder implements Encoder {
	
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
	
	
	
	public <T> void encode(T msg, List<Object> out) throws InvalidProtocolBufferException {
		
		if (msg instanceof MessageLite) {
			out.add(((MessageLite) msg).toByteArray());
	    }
		else if (msg instanceof MessageLite.Builder) {
	    	out.add(((MessageLite.Builder) msg).build().toByteArray());
	    }
		
		throw new InvalidProtocolBufferException(msg.getClass().getName());
	}

}
