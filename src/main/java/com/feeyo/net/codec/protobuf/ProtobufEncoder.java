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
		
		byte[] resultBuffer = new byte[totalSize];
		
		// MAGIC
		resultBuffer[0] = (byte) 0x7f;
		resultBuffer[1] = (byte) 0xff;
		
		// TotalSize
		resultBuffer[2] = (byte) ((totalSize >> 24) & 0xFF);
		resultBuffer[3] = (byte) ((totalSize >> 16) & 0xFF);
		resultBuffer[4] = (byte) ((totalSize >> 8) & 0xFF);
		resultBuffer[5] = (byte) (totalSize & 0xFF);

		System.arraycopy(msgBuffer, 0, resultBuffer, 4, msgBuffer.length);
		return ByteBuffer.wrap(resultBuffer);
	}
	
}
