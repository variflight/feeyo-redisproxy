package com.feeyo.protobuf.codec;

import com.google.protobuf.InvalidProtocolBufferException;

public abstract class Encoder<T> {
	
	public abstract byte[] encode(T obj) throws InvalidProtocolBufferException;

}
