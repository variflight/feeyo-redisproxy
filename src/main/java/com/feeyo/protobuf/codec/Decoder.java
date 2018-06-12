package com.feeyo.protobuf.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public interface Decoder {

	public MessageLite decode(byte[] protobuf) throws InvalidProtocolBufferException;
	
}
