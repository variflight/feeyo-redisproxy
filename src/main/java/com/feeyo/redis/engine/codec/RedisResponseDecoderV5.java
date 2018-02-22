package com.feeyo.redis.engine.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 返回  type+data+\r\n 字节流， 避免 encode   
 * 
 * @see https://redis.io/topics/protocol
 *
 */
public class RedisResponseDecoderV5 extends AbstractDecoder {
	
	
	private List<RedisResponse> responses = null;
	
	public List<RedisResponse> decode(ByteBuffer buffer) {

		append(buffer);

		try {
			if ( responses != null )
				responses.clear();
			else 
				responses = new ArrayList<RedisResponse>(2);
			
			for(;;) {
				
				// 至少4字节  :1\r\n
		        if (bytesRemaining() < 4) {
		          return null;
		        }
				
		        byte type = _buffer.get(_offset++);
		        switch (type) {
		          case '*':					// 数组(Array), 以 "*" 开头,表示消息体总共有多少行（不包括当前行）, "*" 是具体行数
		          case '+':					// 正确, 表示正确的状态信息, "+" 后就是具体信息		
		          case '-':					// 错误, 表示错误的状态信息, "-" 后就是具体信息
		          case ':':					// 整数, 以 ":" 开头, 返回
		          case '$':					// 批量字符串, 以 "$" 开头,表示下一行的字符串长度,具体字符串在下一行中,字符串最大能达到512MB  
		            RedisResponse resp = parseResponse(type);
		            if ( resp != null ) {
		            	responses.add( resp );
		            }
		        }
		        
		        if (_buffer.position() < _offset) {
					throw new IndexOutOfBoundsException("Not enough data.");
					
				} else if (_buffer.position() == _offset) {
					recycleByteBuffer(_buffer);
					_buffer = null;
	            		_offset = 0;
	            		return responses;
				}
			}
			
		} catch (IndexOutOfBoundsException e1) {
			// 捕获这个错误（没有足够的数据），等待下一个数据包 
	        _offset = 0;
	        return null;
		}		
		
	}
	
	private RedisResponse parseResponse(byte type) {

		int start, end, len, offset;
		
		if (type == '+' || type == '-' || type == ':') {			
			
			offset = _offset;			
			start = offset - 1;
			end = readCRLFOffset();	// 分隔符 \r\n			
			_offset = end; 			// 调整偏移值
			
			if (end > _buffer.position() ) {
				_offset = offset;
				throw new IndexOutOfBoundsException("Wait for more data.");
			}
			

			// 长度
			len = end - start;
			return new RedisResponse(type, getBytes(start, len));
			
		 } else if (type == '$') {
			 
			 offset = _offset;
		      
		      // 大小为 -1的数据包被认为是 NULL
		      int packetSize = readInt();
		      if (packetSize == -1) {
		    	start = offset - 1;
		    	end = _offset;
		    	len = end - start;
		        return new RedisResponse(type, getBytes(start, len));  // 此处不减
		      }

		      end = _offset + packetSize + 2;	// offset + data + \r\n
		      _offset = end;					// 调整偏移值		
		      
		      if (end > _buffer.position()) {
		        _offset = offset - 1;
		        throw new IndexOutOfBoundsException("Wait for more data.");
		      }
		      
		      start = offset - 1;
		      len = end - start;
		      return new RedisResponse(type, getBytes(start, len));
			 
		 } else if (type == '*') {
			 
			  offset = _offset;
		    
		      // 大小为 -1的数据包被认为是 NULL
			  int packetSize = readInt();
		      if (packetSize == -1) {
		    	start = offset -1;
		    	end = _offset;
		    	len = end - start;
		        return new RedisResponse(type, getBytes(start, len));  // 此处不减
		      }

		      if (packetSize > bytesRemaining()) {
		        _offset = offset - 1;
		        throw new IndexOutOfBoundsException("Wait for more data.");
		      }

		      // 此处多增加一长度，用于存储 *packetSize\r\n
		      RedisResponse response = new RedisResponse(type, packetSize + 1);
		      start = offset -1;
		      end = _offset;
		      len = end - start;
		      response.set(0, new RedisResponse(type, getBytes(start, len)));

		      byte ntype;
		      RedisResponse res;
		      for (int i = 1; i <= packetSize; i++) {
		        if (_offset + 1 >= _buffer.position() ) {
		          throw new IndexOutOfBoundsException("Wait for more data.");
		        }

		        ntype = _buffer.get(_offset++);
		        res = parseResponse( ntype );
		        response.set(i, res);
		      }
		      return response;
		 }

		return null;
	}
	
	public static void main(String[] args) {
		
	}
	
}