package com.feeyo.redis.engine.codec;

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
public class RedisRequestDecoderV2 {

	private RedisRequest request = null;
	
	private byte[] _buffer;			
	private int _offset;			
	
	
	public void reset() {
		_offset = 0;
		_buffer = null;
	}
	
	public RedisRequest next() throws RedisRequestUnknowException {
		return null;
	}
	
	public RedisRequest decode(byte[] buffer) throws RedisRequestUnknowException {
		
		append( buffer );

 		try {
 			
 			// 至少3字节  :\r\n
	        if (bytesRemaining() < 3) {
	           throw new RedisRequestUnknowException("Invalid request");
	        }
 			
 			// * 协议
 			byte b = _buffer[ _offset ];
 			if ( b == '*') {
 				
 				// skip first byte  *
 				_offset++;
 				
 				request = new RedisRequest();
 				
 				// read arguments count
 				int numArgs = readInt();
				byte[][] args = new byte[ numArgs ][];
				request.setArgs(args);
				
				// read arguments 
				for(int i = 0; i < numArgs; i++ ) { 						
					byte b1 = _buffer[ _offset ];
					if (b1 == '$') {
						_offset++; 
						int length = readInt(); 							
						args[i] = new byte[ length ];
						for(int j = 0; j < length; j++) {
							args[i][j] = _buffer[ _offset + j ];
						}
						
						// skip read length
						_offset += length;
						
						// skip \r\n
						_offset++;
						_offset++; 							
					} else {
						throw new RedisRequestUnknowException("Invalid request");
						//TODO: 异常待处理，抛弃非REDIS包
					}
				}
				
 			// INLINE 协议	
 			} else {
 				
				int start = _offset;
				int end = _offset;
				if (end + 1 >= _buffer.length) {
					throw new IndexOutOfBoundsException("Not enough data.");
				}
				
				while (_buffer[end] != '\r' && _buffer[end + 1] != '\n') {
					end++;
					if (end + 1 == _buffer.length) {
						throw new IndexOutOfBoundsException("didn't see LF");
					}
				}
 			   	
				int length = end - start;
				
				byte[][] args = new byte[1][];
				args[0] = new byte[ length ];
				for(int j = 0; j < length; j++) {
					args[0][j] = _buffer[ _offset + j ];
				}
				
 				request = new RedisRequest();
 				request.setInline( true );
 				request.setArgs(args);
				
				// skip read length
				_offset += length;
				
				// skip \r\n
				_offset++;
				_offset++;
 			}
 			
 			
 			// 处理粘包
			byte[] theBuffer = _buffer;
			if ( theBuffer.length > _offset ) {
				byte[] newBuffer = new byte[ theBuffer.length - _offset ];
				System.arraycopy(theBuffer, _offset, newBuffer, 0, newBuffer.length);
				_buffer = newBuffer;
				theBuffer = null;
				_offset = 0;
				
			} else {
				_offset = 0;
				_buffer = null;
			}

 		} catch (IndexOutOfBoundsException e1) {
 			this._offset = 0;
 			return null;
 		} 	
 		
 		return request;
	}

	private int readInt() throws IndexOutOfBoundsException, RedisRequestUnknowException {		
		
		long v = 0;
		boolean isNeg = false;	
		
		// check offset
		if ( _offset >= _buffer.length ) {
			  throw new IndexOutOfBoundsException("Not enough data.");
		}	
		
		byte b = _buffer[ _offset ];		
		if ( b == '-' )  {
			isNeg = true;			
			_offset++;	
			if ( _offset >= _buffer.length ) {
				  throw new IndexOutOfBoundsException("Not enough data.");
			}		
			b = _buffer[ _offset ];
		}
		
		while ( b != '\r' ) {			
			int value = b - '0';
            if (value >= 0 && value < 10) {
                v *= 10;
                v += value;
            } else {
                throw new RedisRequestUnknowException("Invalid character in integer");
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
		
		v = (isNeg ? -v : v);
		return (int)v;
	}
	
	private void append(byte[] newBuffer) {
		
		if (newBuffer == null) {
			return;
		}

	    if (_buffer == null) {
	      _buffer = newBuffer;
	      return;
	    }
	    
    	_buffer = margeByteArray(_buffer, newBuffer);
	    _offset = 0;
	}	
	
	private byte[] margeByteArray(byte[] a, byte[] b) {
		
	    byte[] result = new byte[a.length + b.length]; 
	    System.arraycopy(a, 0, result, 0, a.length); 
	    System.arraycopy(b, 0, result, a.length, b.length); 
	    
	    /*
	    for (int i = 0; i < result.length; ++i) {
	    	result[i] = i < a.length ? a[i] : b[i - a.length];
    	}
    	*/	    
	    return result;
	} 
	
	private int bytesRemaining() {
	    return (_buffer.length - _offset) < 0 ? 0 : (_buffer.length - _offset);
	}
	
	public static void main(String args[]) throws Exception {
		
		RedisRequestDecoderV2 decoder = new RedisRequestDecoderV2();
		long t = System.currentTimeMillis();
	    for(int j = 0; j < 10000000; j++) {	    	
	    	try {	    		
	    		byte[] buff = "*2\r\n$3\r\nGET\r\n$2\r\naa\r\n".getBytes();
	    		
	    		//buff = "*2\r\n$4\r\nKEYS\r\n$1\r\\*\r\n".getBytes();
	    		
	    		//buff = "PING\r\n".getBytes();
	    		//buff = "QUIT\r\n".getBytes();
	    		//buff = "*1\r\n$4\r\nPING\r\n".getBytes();
	    		
	    		long t1 = System.currentTimeMillis();
				RedisRequest req = decoder.decode( buff  );								
				long t2 = System.currentTimeMillis();
		    	int diff = (int)(t2-t1);
		    	if ( diff > 1) {
		    		System.out.println(" decode diff=" + diff + ", req=" + req.toString() );
		    	}
				
			} catch (RedisRequestUnknowException e) {
				e.printStackTrace();
			}
	    } 
	    System.out.println(System.currentTimeMillis() - t);
	}
}