package com.feeyo.protobuf.codec;

import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;

public interface Encoder {
	
	public <T> byte[] encode( T msg) throws InvalidProtocolBufferException;

	public <T> void encode(T msg, List<byte[]> out) throws InvalidProtocolBufferException;
}
