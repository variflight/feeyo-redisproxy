package com.feeyo.net.codec;

import java.nio.ByteBuffer;

public interface Encoder<T> {

	public ByteBuffer encode(T t);
	
}
