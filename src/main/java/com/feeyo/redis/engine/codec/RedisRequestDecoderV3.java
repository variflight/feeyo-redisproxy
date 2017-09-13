package com.feeyo.redis.engine.codec;

import java.util.ArrayList;
import java.util.List;

/**
 * 支持 新协议
 * 支持 Inline
 * 支持 Pipeline
 * 
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
public class RedisRequestDecoderV3 {

	private RedisRequest request = null;
	
	private byte[] _buffer;			
	private int _offset;			
		
	public void reset() {
		_offset = 0;
		_buffer = null;
	}
	
	public List<RedisRequest> decode(byte[] buffer) throws RedisRequestUnknowException {
		
		append( buffer );
		
		// pipeline
		List<RedisRequest> pipeline = new ArrayList<RedisRequest>();

 		try {
 			
 			for(;;) {
 				
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
					
					//
					pipeline.add( request );
					
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
	 				
	 				// 
	 				pipeline.add( request );
					
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
					
					// 整包解析完成
					return pipeline;
				}
				
 			}
 			
 		} catch (IndexOutOfBoundsException e1) {
 			this._offset = 0;
 			return null;
 		} 	
 		
 		
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
	
	
	/**
	    2a 31 0d 0a 24 35 0d 0a     * 1 . . $ 5 . . 
		4d 55 4c 54 49 0d 0a 2a     M U L T I . . * 
		32 0d 0a 24 34 0d 0a 4c     2 . . $ 4 . . L 
		4c 45 4e 0d 0a 24 36 0d     L E N . . $ 6 . 
		0a 63 65 6c 65 72 79 0d     . c e l e r y . 
		0a 2a 32 0d 0a 24 34 0d     . * 2 . . $ 4 . 
		0a 4c 4c 45 4e 0d 0a 24     . L L E N . . $ 
		39 0d 0a 63 65 6c 65 72     9 . . c e l e r 
		79 06 16 33 0d 0a 2a 32     y . . 3 . . * 2 
		0d 0a 24 34 0d 0a 4c 4c     . . $ 4 . . L L 
		45 4e 0d 0a 24 39 0d 0a     E N . . $ 9 . . 
		63 65 6c 65 72 79 06 16     c e l e r y . . 
		36 0d 0a 2a 32 0d 0a 24     6 . . * 2 . . $ 
		34 0d 0a 4c 4c 45 4e 0d     4 . . L L E N . 
		0a 24 39 0d 0a 63 65 6c     . $ 9 . . c e l 
		65 72 79 06 16 39 0d 0a     e r y . . 9 . . 
		2a 31 0d 0a 24 34 0d 0a     * 1 . . $ 4 . . 
		45 58 45 43 0d 0a           E X E C . . 
	 */
	public static void main(String args[]) throws Exception {
		
		RedisRequestDecoderV3 decoder = new RedisRequestDecoderV3();
		
	    for(int j = 0; j < 1; j++) {	    	
	    	try {	    		
	    		byte[] buff = "*2\r\n$3\r\nGET\r\n$2\r\naa\r\n".getBytes();
	    		buff = "*1\r\n$5\r\nMULTI\r\n*2\r\n$4\r\nLLEN\r\n$6\r\ncelery\r\n*1\r\n$4\r\nEXEC\r\n".getBytes();
	    		//buff = "*2\r\n$4\r\nKEYS\r\n$1\r\\*\r\n".getBytes();
	    		//buff = "QUIT\r\n".getBytes();
	    		//buff = "*1\r\n$4\r\nPING\r\n".getBytes();
	    		
	    		long t1 = System.currentTimeMillis();
				List<RedisRequest> reqs = decoder.decode( buff  );								
				long t2 = System.currentTimeMillis();
		    	int diff = (int)(t2-t1);
		    	if ( diff > 1) {
		    		System.out.println(" decode diff=" + diff + ", req=" + reqs.toString() );
		    	}
				
			} catch (RedisRequestUnknowException e) {
				e.printStackTrace();
			}
	    }  
	}
}