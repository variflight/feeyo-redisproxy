package com.feeyo.net.codec;

public interface Decoder<T> {

	public T decode(byte[] buffer) throws UnknowProtocolException;
	
}
