package com.feeyo.redis.engine.codec;

/**
 * TODO: 暂不考虑支持原始的 INLINE 及  Bulk
 * 
	----------------------Redis 协议----------------------- 
	*<参数数量> CR LF
	$<参数1的字节数> CR LF
	<参数1的数据> CR LF
	...
	$<参数N的字节数> CR LF
	<参数N的数据> CR LF
	--------------------------------------------------------
*/
public class RedisRequestDecoderV1 {
	
	private enum State {
		READ_SKIP, 			// 健壮性考虑，如果第一个字符不是*则skip直到遇到*
		READ_INIT, 			// 开始
		READ_ARG_LENGTH, 	// 参数数量
		READ_ARG,  			// 参数
		READ_END			// 结束
	}
	
	private RedisRequest request = null;
	
	private State state = null;	
	private byte[] _buffer;			
	private int _offset;			
	
	public RedisRequestDecoderV1() {
		this.state = State.READ_SKIP;
	}
	
	public void reset() {
		_offset = 0;
		_buffer = null;
	}
	
	public RedisRequest decode(byte[] buffer) throws RedisRequestUnknowException {
		
		append( buffer );

	    // 根据状态解析 redis 协议
 		try {
 			switch (state) {
 				case READ_SKIP: {
					skipByte();
					this.state = State.READ_INIT;
				}
 				case READ_INIT: {
 					byte b = _buffer[ _offset ];
 					if (b == '*') { 			// redis 协议开头
 						_offset++;
 						b = _buffer[_offset ];
 						if (b == '\r') { 			
 							_offset++;
 							this.state = State.READ_SKIP;
 						} else {
 							request = new RedisRequest();
 							this.state = State.READ_ARG_LENGTH;
 						}
 					}
 				}
 				case READ_ARG_LENGTH: {
 					int numArgs = readInt();
 					byte[][] args = new byte[ numArgs ][];
 					request.setArgs( args );
 					this.state = State.READ_ARG;
 				}
 				case READ_ARG: {
 					byte[][] args = request.getArgs();
 					for(int i = 0; i < request.getNumArgs(); i++ ) { 						
 						byte b = _buffer[ _offset ];
 						if (b == '$') {
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
 							throw new RedisRequestUnknowException();
 							//TODO: 异常待处理，抛弃非REDIS包
 						}
 					}
					this.state = State.READ_END;
 				}
 				case READ_END: {
 					this.state = State.READ_SKIP;
 					
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
 				}
 			} 		

 		} catch (IndexOutOfBoundsException e1) {
 			this.state = State.READ_SKIP;
 			this._offset = 0;
 			return null;
 		} 		
 		return request;
	}
	
	private void skipByte() throws IndexOutOfBoundsException {
		for (;;) {			
			if ( _offset >= _buffer.length ) {
				  throw new IndexOutOfBoundsException("Not enough data.");
			}	
			
			byte b = _buffer[ _offset ];
			if (b == '*') {
				break;
			}
			_offset++;
		}
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
	
	public static void main(String args[]) throws Exception {
		
		RedisRequestDecoderV1 decoder = new RedisRequestDecoderV1();
		
	    for(int j = 0; j < 5000000; j++) {	    	
	    	try {	    		
	    		byte[] buff = "*2\r\n$3\r\nGET\r\n$2\r\naa\r\n".getBytes();
	    		//buff = "*2\r\n$4\r\nKEYS\r\n$1\r\\*\r\n".getBytes();
	    		buff = "*1\r\n$4\r\nPING\r\n".getBytes();
	    		
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
	}
}