package com.feeyo.redis.engine.codec;

/**
 * 返回  type+data+\r\n 字节流， 避免 encode   
 * 
 * @see https://redis.io/topics/protocol
 *
 */
public class RedisResponseDecoderV3 {
	
	private byte[] _buffer;
	private int _offset;
	
	public RedisResponse decode(byte[] buffer) {

		append(buffer);

		try {
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
	            	_buffer = null;
	            	_offset = 0;
	            }
	            return resp;
	        }
			
		} catch (IndexOutOfBoundsException e1) {
			// 捕获这个错误（没有足够的数据），等待下一个数据包 
	        _offset = 0;
		}		
		return null;
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
			len = end - start + 1;
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
		
		buffer = "*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_aa\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_zz\r\n:2\r\n".getBytes();

		
		byte[] buffer1 = new byte[ buffer.length / 2 ];
		byte[] buffer2 = new byte[ buffer.length - buffer1.length ];
		
		System.arraycopy(buffer, 0, buffer1, 0, buffer1.length);
		System.arraycopy(buffer, buffer1.length, buffer2, 0, buffer2.length);
		System.arraycopy(buffer, buffer1.length, buffer2, 0, buffer2.length);
		
		RedisResponseDecoderV3 decoder = new RedisResponseDecoderV3();
		RedisResponse resp = decoder.decode(buffer);
		
		//RedisResponseV3 resp = decoder.decode(buffer1);
		//resp = decoder.decode(buffer2);
		
		System.out.println( resp );
	}
	
}