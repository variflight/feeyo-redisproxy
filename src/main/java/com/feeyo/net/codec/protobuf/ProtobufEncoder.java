package com.feeyo.net.codec.protobuf;

import java.nio.ByteBuffer;

import com.feeyo.net.codec.Encoder;
import com.feeyo.util.ByteUtil;
import com.google.protobuf.MessageLite;

public class ProtobufEncoder implements Encoder<MessageLite>{
	
	private static final byte[] MAGIC_CODE = new byte[] { (byte) 0x7f, (byte) 0xff };
	
	// ==== wrap-index ===== size ===========
	// totalSize 			4
	// content 				content.length
	// MAGIC_CODE 			MAGIC_CODE.length

	@Override
	public ByteBuffer encode(MessageLite msg){

		if(msg == null)
			return null;
		
		byte[] content = null;
		byte[] ret = null;
			
		content = msg.toByteArray();
		int totalSize = 4 + content.length + MAGIC_CODE.length;

		if (totalSize == 0 || content == null || content.length == 0)
			return null;
		
		ret = new byte[totalSize];

		System.arraycopy(ByteUtil.intToBytes(totalSize), 0, ret, 0, 4);
		System.arraycopy(content, 0, ret, 4 , content.length);
		System.arraycopy(MAGIC_CODE, 0, ret, 4 + content.length, MAGIC_CODE.length);
		return ByteBuffer.wrap(ret);
		
	}
	
}
