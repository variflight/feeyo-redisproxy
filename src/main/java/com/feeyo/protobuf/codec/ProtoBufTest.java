package com.feeyo.protobuf.codec;

import java.nio.charset.Charset;

import com.feeyo.protobuf.codec.ProtoBufMessage.RedisMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProtoBufTest {

	public static void main(String[] args) {

		RedisMessage fromMsg = RedisMessage.newBuilder().setId(1).setIndex(2)
				.setContent(ByteString.copyFrom("hello protobuf!", Charset.forName("UTF-8"))).build();

		ProtoBufEncoder encoder = new ProtoBufEncoder();

		byte[] protobuf;
		
		try {
			protobuf = encoder.encode(fromMsg);
			RedisMessage toMsg = RedisMessage.newBuilder().build();
			ProtoBufDecoder decoder = new ProtoBufDecoder(toMsg);
			toMsg = (RedisMessage) decoder.decode(protobuf);
		} catch (InvalidProtocolBufferException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
