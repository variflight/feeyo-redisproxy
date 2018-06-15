package com.feeyo.net.codec.redis;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.Encoder;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.util.ProtoUtils;

/**
----------------------Redis 协议----------------------- 
	*<参数数量> CR LF
	$<参数1的字节数> CR LF
	<参数1的数据> CR LF
	...
	$<参数N的字节数> CR LF
	<参数N的数据> CR LF
--------------------------------------------------------
*/
public class RedisRequestEncoder implements Encoder<byte[][]> {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RedisRequestEncoder.class );
	
	private static final byte ASTERISK = '*';
	private static final byte DOLLAR = '$';
	
	private static final byte[] CRLF = "\r\n".getBytes();		
	
	@Override
	public ByteBuffer encode(byte[][] args) {
		
		if ( args == null ) {
			LOGGER.warn("encode err: args is null");
			return null;
		}		
		
		byte[][] lens = new byte[ args.length + 1 ][];
		lens[0] = ProtoUtils.convertIntToByteArray( args.length );
		
		// 计算 bufferSize
		int bufferSize = 1 + 2 + lens[0].length;
		for(int i = 0; i < args.length; i++) {
			lens[i+1] = ProtoUtils.convertIntToByteArray( args[i].length );
			bufferSize = bufferSize + ( args[i].length + 5 + lens[i+1].length );  // DOLLAR, CRLF, CRLF, LEN
		}
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( bufferSize );
		try {	
			
			buffer.put( ASTERISK );
			buffer.put( lens[0] );
			buffer.put( CRLF );
			for (int i = 0; i < args.length; i++) {  
				buffer.put( DOLLAR );
				buffer.put( lens[i+1] ); 
				buffer.put( CRLF );
				buffer.put( args[i] );  
				buffer.put( CRLF );
			}		
			
			// fast GC
			lens = null;
			
			return buffer;	
			
		} catch(BufferOverflowException e) {			
			try {
				StringBuffer msgBuffer = new StringBuffer();
				for (int i = 0; i < args.length; i++) {  
					msgBuffer.append( new String( args[i] ) ).append("\r\n");  
				}	
				LOGGER.warn("request enc err: culc size={}, msg={}, buffer limit={}, capacity={}, postion={}",
						new Object[] { bufferSize, msgBuffer.toString(), buffer == null ? 0 : buffer.limit(),
								buffer == null ? 0 : buffer.capacity(), buffer == null ? 0 : buffer.position() });
			} catch (Exception ee) {
			} finally {
				// 回收
				NetSystem.getInstance().getBufferPool().recycle( buffer );
			}
			
			// 继续往上抛出异常
			throw e;
		}
	}	
	
	public ByteBuffer encode(List<RedisRequest> RedisRequests) {
		
		if ( RedisRequests == null ) {
			LOGGER.warn("encode err: RedisRequests is null");
			return null;
		}		
		int bufferSize = 0;
		for (RedisRequest request : RedisRequests) {
			byte[][] lens = new byte[ request.getArgs().length + 1 ][];
			lens[0] = ProtoUtils.convertIntToByteArray( request.getArgs().length );
			
			// 计算 bufferSize
			bufferSize = bufferSize + 1 + 2 + lens[0].length;
			for(int i = 0; i < request.getArgs().length; i++) {
				lens[i+1] = ProtoUtils.convertIntToByteArray( request.getArgs()[i].length );
				bufferSize = bufferSize + ( request.getArgs()[i].length + 5 + lens[i+1].length );  // DOLLAR, CRLF, CRLF, LEN
			}
			lens = null;
		}
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( bufferSize );
		for (RedisRequest request : RedisRequests) {
			
			byte[][] lens = new byte[ request.getArgs().length + 1 ][];
			lens[0] = ProtoUtils.convertIntToByteArray( request.getArgs().length );
			
			for(int i = 0; i < request.getArgs().length; i++) {
				lens[i+1] = ProtoUtils.convertIntToByteArray( request.getArgs()[i].length );
			}
			
			try {	
				
				buffer.put( ASTERISK );
				buffer.put( lens[0] );
				buffer.put( CRLF );
				for (int i = 0; i < request.getArgs().length; i++) {  
					buffer.put( DOLLAR );
					buffer.put( lens[i+1] ); 
					buffer.put( CRLF );
					buffer.put( request.getArgs()[i] );  
					buffer.put( CRLF );
				}		
				
				// fast GC
				lens = null;
				//request.clear();
				request = null;
			} catch(BufferOverflowException e) {		
				
				try {
					StringBuffer msgBuffer = new StringBuffer();
					for (int i = 0; i < request.getArgs().length; i++) {  
						msgBuffer.append( new String( request.getArgs()[i] ) ).append("\r\n");  
					}	
					
					LOGGER.warn("request enc err: culc size={}, msg={}, buffer limit={}, capacity={}, postion={}",
							new Object[] { bufferSize, msgBuffer.toString(), buffer == null ? 0 : buffer.limit(),
									buffer == null ? 0 : buffer.capacity(), buffer == null ? 0 : buffer.position() });
				} catch (Exception ee) {
				} finally {
					// 回收
					NetSystem.getInstance().getBufferPool().recycle( buffer );
				}
				// 继续往上抛出异常
				throw e;
			}
		}
		return buffer;	
	}	
}