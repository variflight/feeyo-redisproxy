package com.feeyo.redis.engine.codec;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import com.feeyo.redis.nio.NetSystem;
import com.feeyo.util.ProtoUtils;

public class RedisResponseEncoderV2 {
	
	public static final byte DOLLAR_BYTE = '$';
	public static final byte ASTERISK_BYTE = '*';
	public static final byte PLUS_BYTE = '+';		// STATUS		
	public static final byte MINUS_BYTE = '-';		// ERROR
	public static final byte COLON_BYTE = ':';		// INTEGER
	
	public static final byte[] CRLF_BYTE = "\r\n".getBytes();
	
	// 考虑 
	public byte[] encode(RedisResponse response) {
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( 1024 );
		
		outer: 
		for(;;) {
			try {
				encode(buffer, response);
			} catch(BufferOverflowException e) {	
				// 重新分配
				ByteBuffer newBuffer = NetSystem.getInstance().getBufferPool().allocate(  buffer.capacity() * 2 );
				NetSystem.getInstance().getBufferPool().recycle( buffer );			
				buffer = newBuffer;
				continue outer;
			}
			break;
		}
		
		byte[] bb = null;
		try {
			buffer.flip();
			bb = new byte[ buffer.remaining() ];
			buffer.get(bb, 0, bb.length);
		} finally {
			NetSystem.getInstance().getBufferPool().recycle( buffer );		
		}
		
		return bb;
	}
	
	private void encode(ByteBuffer buffer, RedisResponse item) throws BufferOverflowException {
		
		if ( item == null ) {
			return;
		}
		
		if ( item.type() == '+' ) {
			
			byte[] bb = (byte[])item.data();
			
			buffer.put( PLUS_BYTE );
			buffer.put( bb );
			buffer.put( CRLF_BYTE );

			
		} else if ( item.type() == '-') {
			
			byte[] bb = (byte[])item.data();
			
			buffer.put( MINUS_BYTE );
			buffer.put( bb );
			buffer.put( CRLF_BYTE );
			
		} else if ( item.type() == ':') {
			
			byte[] bb = (byte[])item.data();
			
			buffer.put( COLON_BYTE );
			buffer.put( bb );
			buffer.put( CRLF_BYTE );
			
		} else if ( item.type() == '$') {
			
			if ( item.data() != null ) {				
				byte[] bb = (byte[])item.data();
				
				buffer.put( DOLLAR_BYTE );
				buffer.put(  ProtoUtils.convertIntToByteArray( bb.length ) );
				buffer.put( CRLF_BYTE );
				buffer.put( bb );
				buffer.put( CRLF_BYTE );
				
			} else {				
				buffer.put( DOLLAR_BYTE );
				buffer.put(  ProtoUtils.convertIntToByteArray( -1 ) );
				buffer.put( CRLF_BYTE );
			}

			
		} else if ( item.type() == '*') {			
			
			if ( item.data() != null ) {	
				
				RedisResponse[] items = (RedisResponse[]) item.data();				
				buffer.put( ASTERISK_BYTE );
				buffer.put(  ProtoUtils.convertIntToByteArray( items.length ) );
				buffer.put( CRLF_BYTE );
				
				for(int i = 0; i < items.length; i++) {
					encode(buffer, items[i] );
				}
				
			} else {				
				buffer.put( ASTERISK_BYTE );
				buffer.put(  ProtoUtils.convertIntToByteArray( -1 ) );
				buffer.put( CRLF_BYTE );
			}
		}
	}

}
