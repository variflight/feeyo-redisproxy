package com.feeyo.net.codec.protobuf;

import java.nio.ByteBuffer;

import com.feeyo.net.codec.Encoder;
import com.google.protobuf.MessageLite;

public class ProtobufEncoder implements Encoder<MessageLite>{

	@Override
	public ByteBuffer encode(MessageLite msg){

		if (msg == null)
			return null;

		byte[] msgBuffer = msg.toByteArray();
		if (msgBuffer == null || msgBuffer.length == 0) {
			return null;
		}
		
		//
		int totalSize = 2 + 4 + msgBuffer.length;
		if (totalSize == 0 || msgBuffer == null || msgBuffer.length == 0)
			return null;
		
		ByteBuffer buffer = ByteBuffer.allocate(totalSize);
		
		// MAGIC
		buffer.put( (byte)0x7f );
		buffer.put( (byte)0xff );
		
		// TOTALSIZE
		buffer.put( (byte) ((totalSize >> 24) & 0xFF) );
		buffer.put( (byte) ((totalSize >> 16) & 0xFF) );
		buffer.put( (byte) ((totalSize >> 8) & 0xFF) );
		buffer.put( (byte) (totalSize & 0xFF) );
		
		buffer.put( msgBuffer );

		return buffer;
	}
	
}
