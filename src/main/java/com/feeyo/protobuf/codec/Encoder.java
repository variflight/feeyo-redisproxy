package com.feeyo.protobuf.codec;

import com.google.protobuf.InvalidProtocolBufferException;

public interface Encoder {
	
	public <T> byte[] encode( T msg) throws InvalidProtocolBufferException;

}
