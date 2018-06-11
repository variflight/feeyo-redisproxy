package com.feeyo.protobuf.codec;

public abstract class Decoder {

	public abstract <T> T decode(Class<T> clazz, byte[] protobuf);
	
}
