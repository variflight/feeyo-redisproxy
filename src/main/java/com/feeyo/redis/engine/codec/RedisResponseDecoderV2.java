package com.feeyo.redis.engine.codec;

/**
 * 
 * @author zhuam
 *
 */
public class RedisResponseDecoderV2 {
	
	private byte[] _buffer;
	private int _offset;
	

	/**
	 --------------------keys-------------------------
	 req:
	 	keys *	 		
	 resp:
 		*2\r\n
 		$7\r\n
 		pre1_bb\r\n
 		$7\r\n
 		pre1_aa\r\n
	 		
	 -------------------set--------------------------
	 req:
	 	set cc 1cccccccccc9
	 resp:
	 	+OK\r\n
	 	
	 -------------------get--------------------------
	 req:
	 	get cc
	 resp:
	 	$12\r\n
	 	1cccccccccc9\r\n
	 
	 -------------------del--------------------------
	 req:
	 	del cc
	 resp:
	 	:1\r\n
	 
	 -------------------get--------------------------
	 req:
	 	get cc
	 resp:
	 	$-1\r\n
	 	
	 */
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
	        	  
	            final RedisResponse resp = parseResponse(type);
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
		int packetSize;
		if (type == '+' || type == '-') {
			// 到分隔符
			end = readCRLFOffset() - 1;
			start = _offset;
			
			// 包括分隔符
			_offset = end + 2;

			if (end > _buffer.length ) {
				_offset = start;
				throw new IndexOutOfBoundsException("Wait for more data.");
			}
			
			len = end - start;
			return new RedisResponse(type, getBytes(start, len));
			
		 } else if (type == ':') {
			 
			  // 到分隔符
		      end = readCRLFOffset() - 1;
		      start = _offset;

		      // 包括分隔符
		      _offset = end + 2;

		      if (end > _buffer.length ) {
		        _offset = start;
		        throw new IndexOutOfBoundsException("Wait for more data.");
		      }

		      len = end - start;
		      // return the coerced numeric value
		      return new RedisResponse(type, getBytes(start, len));
		      
		 } else if (type == '$') {
			 
			 // 由于数据包可能大于内存中的缓冲区, 所以设置一个回退的标记点
		      offset = _offset - 1;
		      
		      packetSize = readInt();

		      // 大小为 -1的数据包被认为是 NULL
		      if (packetSize == -1) {
		        return new RedisResponse(type, null);
		      }

		      end = _offset + packetSize;
		      start = _offset;

		      // 设置偏移后的分隔符
		      _offset = end + 2;

		      if (end > _buffer.length) {
		        _offset = offset;
		        throw new IndexOutOfBoundsException("Wait for more data.");
		      }
		      
		      len = end - start;
		      return new RedisResponse(type, getBytes(start, len));
			 
		 } else if (type == '*') {
			 
			  offset = _offset;
		      packetSize = readInt();

		      // 大小为 -1的数据包被认为是 NULL
		      if (packetSize == -1) {
		        return new RedisResponse(type, null);
		      }

		      if (packetSize > bytesRemaining()) {
		        _offset = offset - 1;
		        throw new IndexOutOfBoundsException("Wait for more data.");
		      }

		      RedisResponse response = new RedisResponse(type, packetSize);

		      byte ntype;
		      RedisResponse res;
		      for (int i = 0; i < packetSize; i++) {
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
		int end = readCRLFOffset();
		int len = end - 1 - _offset;
		
		long size = 0;
		byte[] buf = getBytes(_offset, len);
		boolean isNeg = false;
		for (byte b : buf) {
			if ( b == '-') {
				isNeg = true;
			} else {
				size = size * 10 + b - '0';
			}
		}
		
		size = (isNeg ? -size : size);
		if (size > Integer.MAX_VALUE) {
			throw new RuntimeException("Cannot allocate more than " + Integer.MAX_VALUE + " bytes");
		}
		if (size < Integer.MIN_VALUE) {
			throw new RuntimeException("Cannot allocate less than " + Integer.MIN_VALUE + " bytes");
		}
		
		_offset = end + 1;

		return (int) size;
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
	
	/*
	private String getString(int offset, int length) {
		return getString(offset, length, null);
	}
	
	private String getString(int offset, int length, java.nio.charset.Charset charset) {
		byte[] bb = getBytes(offset, length);
		if ( charset != null ) {
			return new String(bb, charset);
		} else {
			return new String(bb);
		}	
	}
	*/
	
	public static void main(String[] args) {
		
		byte[] buffer = "+PONG \r\n".getBytes();
		buffer = "$12\r\n1cccccccccc9\r\n".getBytes();
		buffer = "*2\r\n$7\r\npre1_bb\r\n$7\r\npre1_aa\r\n".getBytes();
		
		byte[] buffer1 = new byte[ buffer.length / 2 ];
		byte[] buffer2 = new byte[ buffer.length - buffer1.length ];
		
		System.arraycopy(buffer, 0, buffer1, 0, buffer1.length);
		System.arraycopy(buffer, buffer1.length, buffer2, 0, buffer2.length);
		
		RedisResponseDecoderV2 decoder = new RedisResponseDecoderV2();
		//RedisResponse resp = decoder.decode(buffer);
		RedisResponse resp = decoder.decode(buffer1);
		resp = decoder.decode(buffer2);
		
		System.out.println( resp );
	}
	
}