package com.feeyo.net.codec.protobuf;

import java.nio.ByteBuffer;

import com.feeyo.net.codec.Encoder;
import com.google.protobuf.MessageLite;

/**
 * 
 * @author xuwenfeng
 *
 */
public class ProtobufEncoder implements Encoder<MessageLite>{
	
	private boolean isCustomPkg = false;
	
	public ProtobufEncoder(boolean isCustomPkg) {
		this.isCustomPkg = isCustomPkg;
	}

	@Override
	public ByteBuffer encode(MessageLite msg){

		if (msg == null)
			return null;

		byte[] msgBuffer = msg.toByteArray();
		if (msgBuffer == null || msgBuffer.length == 0) {
			return null;
		}
		
		if ( !isCustomPkg ) {
			return ByteBuffer.wrap(msgBuffer);
		}
		
		//
		int totalSize =  4 + msgBuffer.length;
		ByteBuffer buffer = ByteBuffer.allocate(totalSize);
		buffer.put( (byte) ((totalSize >> 24) & 0xFF) );
		buffer.put( (byte) ((totalSize >> 16) & 0xFF) );
		buffer.put( (byte) ((totalSize >> 8) & 0xFF) );
		buffer.put( (byte) (totalSize & 0xFF) );
		//
		buffer.put( msgBuffer );

		return buffer;
	}
	
}
