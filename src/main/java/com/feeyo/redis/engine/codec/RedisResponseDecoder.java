package com.feeyo.redis.engine.codec;

import java.util.ArrayList;
import java.util.List;

/**
 * 返回  type+data+\r\n 字节流， 避免 encode   
 * 
 * @see https://redis.io/topics/protocol
 *
 */
public class RedisResponseDecoder {
	
	private byte[] _buffer;
	private int _offset;
	
	private List<RedisResponse> responses = null;
	
	public List<RedisResponse> decode(byte[] buffer) {

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
				
		        byte type = _buffer [ _offset++ ];
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
		        
		        if (_buffer.length < _offset) {
					throw new IndexOutOfBoundsException("Not enough data.");
					
				} else if (_buffer.length == _offset) {
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
			
			if (end > _buffer.length ) {
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
		      
		      if (end > _buffer.length) {
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
		        if (_offset + 1 >= _buffer.length ) {
		          throw new IndexOutOfBoundsException("Wait for more data.");
		        }

		        ntype = _buffer [ _offset++ ];
		        res = parseResponse( ntype );
		        response.set(i, res);
		      }
		      return response;
		 }

		return null;
	}
	
	private int bytesRemaining() {
	    return (_buffer.length - _offset) < 0 ? 0 : (_buffer.length - _offset);
	}

	private int readInt() throws IndexOutOfBoundsException {		

		long size = 0;
		boolean isNeg = false;

		if ( _offset >= _buffer.length ) {
			  throw new IndexOutOfBoundsException("Not enough data.");
		}	
		
		byte b = _buffer[ _offset ];		
		while ( b != '\r' ) {			
			if ( b == '-') {
				isNeg = true;
			} else {
				size = size * 10 + b - '0';
			}			
			_offset++;
			
			if ( _offset >= _buffer.length ) {
				  throw new IndexOutOfBoundsException("Not enough data.");
			}			
			b = _buffer[ _offset ];
		}	
		
		// skip \r\n
		_offset++;
		_offset++;
		
		size = (isNeg ? -size : size);
		if (size > Integer.MAX_VALUE) {
			throw new RuntimeException("Cannot allocate more than " + Integer.MAX_VALUE + " bytes");
		}
		if (size < Integer.MIN_VALUE) {
			throw new RuntimeException("Cannot allocate less than " + Integer.MIN_VALUE + " bytes");
		}
		
		return (int)size;
	}
	
	private int readCRLFOffset() throws IndexOutOfBoundsException {
		int offset = _offset;
		
	    if (offset + 1 >= _buffer.length ) {
	      throw new IndexOutOfBoundsException("Not enough data.");
	    }
	    
	    while ( _buffer[ offset ] != '\r' && _buffer[ offset + 1 ] != '\n') {
	      offset++;
	      if ( offset + 1 == _buffer.length ) {
	        throw new IndexOutOfBoundsException("didn't see LF after NL reading multi bulk count (" 
	        			+ offset + " => " + _buffer.length + ", " + _offset + ")");	        
	      }
	    }
	    offset++;
	    offset++;
	    return offset;
	}
	
	private void append(byte[] newBuffer) {
		
		if (newBuffer == null) {
			return;
		}
		
	    if (_buffer == null) {
	      _buffer = newBuffer;
	      return;
	    }
	    
	    // large packet	    	
    	byte[] largeBuffer = new byte[ _buffer.length + newBuffer.length ];
    	System.arraycopy(_buffer, 0, largeBuffer, 0, _buffer.length);
    	System.arraycopy(newBuffer, 0, largeBuffer, _buffer.length, newBuffer.length);
    	
    	_buffer = largeBuffer;
	    _offset = 0;
	}	
	
	private byte[] getBytes(int offset, int length) {
		byte[] arr = new byte[ length ];
		System.arraycopy(_buffer, offset, arr, 0, length);
		return arr;
	}
	
	public static void main(String[] args) {
		
		byte[] buffer = "+PONG \r\n".getBytes();
		buffer = "-ERR Not implemented\r\n".getBytes();
		//buffer = "*-1\r\n".getBytes();
		//buffer = "$12\r\n1cccccccccc9\r\n".getBytes();
		//buffer = "$-1\r\n".getBytes();
		buffer = "*2\r\n$7\r\npre1_bb\r\n$7\r\npre1_aa\r\n".getBytes();
		
//		buffer = "*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_aa\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_zz\r\n:2\r\n".getBytes();
		
		buffer = ("*3\r\n*4\r\n:5461\r\n:10922\r\n*3\r\n$15\r\n192.168.219.131\r\n:7003\r\n$40\r\n1fd8af2aa246c5adf00a25d1b6a0c1f4743bae5c\r\n"
				+ "*3\r\n$15\r\n192.168.219.132\r\n:7002\r\n$40\r\ne0b1c5791694fdc2ede655023e80f0e57b3d86b4\r\n*4\r\n:0\r\n:5460\r\n"
				+ "*3\r\n$15\r\n192.168.219.132\r\n:7000\r\n$40\r\nbee6866a13093c4411dea443ca8d901ea5d1e2f3\r\n"
				+ "*3\r\n$15\r\n192.168.219.131\r\n:7004\r\n$40\r\nb3ba9c1af0fa7296fe1e32f2a955879bcf79108b\r\n*4\r\n:10923\r\n:16383\r\n"
				+ "*3\r\n$15\r\n192.168.219.132\r\n:7001\r\n$40\r\n9c86ec8088050f837c490aeda15aca5a2c85d7ef\r\n"
				+ "*3\r\n$15\r\n192.168.219.131\r\n:7005\r\n$40\r\nb0e22eccf79ced356e54a92ecbaa8d22757765d4\r\n").getBytes();
		
		byte[] buff = new byte[ buffer.length * 2 ];
		System.arraycopy(buffer, 0, buff, 0, buffer.length);
		System.arraycopy(buffer, 0, buff, buffer.length, buffer.length);
		buffer = buff;
		System.out.println(buffer.length);
		System.out.println(buff.length);
		byte[] buffer1 = new byte[ buffer.length / 3 ];
		byte[] buffer2 = new byte[ buffer.length - buffer1.length ];
		
		System.arraycopy(buffer, 0, buffer1, 0, buffer1.length);
		System.arraycopy(buffer, buffer1.length, buffer2, 0, buffer2.length);
		System.arraycopy(buffer, buffer1.length, buffer2, 0, buffer2.length);
		
		RedisResponseDecoder decoder = new RedisResponseDecoder();
		//List<RedisResponseV3> resps = decoder.decode(buffer);
		
		List<RedisResponse> resps = decoder.decode(buffer);
		System.out.println( resps );
		resps = decoder.decode(buffer2);
//		System.out.println( resps );
	}
	
}