package com.feeyo.redis.engine.codec;

import java.util.ArrayList;
import java.util.List;

/**
 * 支持 新协议
 * 支持 Inline
 * 支持 Bulk 
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
public class RedisRequestDecoderV4 {

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
 				
 				// 跳过开头的空格
 				skipBytes();
	 			
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
					
					// 命令数组长度
					int argsSize = 0;
					byte[][] args = null;
					
					// 循环_offset。分解 命令  key value。start指针指向数据开始的位置，end指向数据结束的位置（注意空格处理）。
					while (_buffer[end] != '\r' && _buffer[end + 1] != '\n') {
						// 开始循环
						if (end == start) {
							if (_buffer[start] == ' ') {
								start++;
							} 
							end++;
						} else if (_buffer[end] != ' ') {
							// 数据没有结束， end后移。
							end++;
						} else {
							// 分段数据结束，args增加一行数据
							argsSize++;
							// argsBuffer用于存储老的args数据，并加上刚提取的数据。最后赋值给args
							byte[][] argsBuffer = new byte[argsSize][];
							for (int i = 0; i < argsSize - 1; i++) {
								argsBuffer[i] = args[i];
							}
							// 复制刚提取的数据
							argsBuffer[argsSize - 1] = new byte[end - start];
							System.arraycopy(_buffer, start, argsBuffer[argsSize - 1], 0, argsBuffer[argsSize - 1].length);
							args = argsBuffer;
							end++;
							// 同步指针，开始提取新的数据
							start = end;
						}
						if (end + 1 == _buffer.length) {
							throw new IndexOutOfBoundsException("didn't see LF");
						}
					}
					// 如果 \r\n 之前不是空格。提取最后一段数据
					if (start < end) {
						// args增加一行数据
						argsSize++;
						// argsBuffer用于存储老的args数据，并加上刚提取的数据。最后赋值给args
						byte[][] argsBuffer = new byte[argsSize][];
						for (int i = 0;i<argsSize - 1;i++) {
							argsBuffer[i] = args[i];
						}
						argsBuffer[argsSize - 1] = new byte[end - start];
						System.arraycopy(_buffer, start, argsBuffer[argsSize - 1], 0, argsBuffer[argsSize - 1].length);
						args = argsBuffer;
					}
					
	 				request = new RedisRequest();
	 				request.setInline( true );
	 				request.setArgs(args);
	 				// 
	 				pipeline.add( request );
					
					// skip read length
					_offset = end;
					
					// skip \r\n
					_offset++;
					_offset++;
					
					// 进一步处理 考虑  bulk 命令
					if (_offset < _buffer.length) {
						try {
							// 得到需要替换的value长度
							int dateLength = getBulkValueLength(args[argsSize - 1]);
							if (dateLength > _buffer.length - _offset - 2) {
								throw new IndexOutOfBoundsException("Not enough data.");
							}
							// 判断数据结尾是否是\r\n 防止inline粘包。
							if (_buffer[dateLength + _offset] == '\r' && _buffer[dateLength + _offset + 1] == '\n') {
								// 替换value。
								args[argsSize - 1] = new byte[dateLength];
								System.arraycopy(_buffer, _offset, args[argsSize - 1], 0, dateLength);
								_offset += dateLength;
								_offset++;
								_offset++;
							}
						} catch (RedisRequestUnknowException e) {
							// 异常说明不是bulk命令，可能是inline粘包
						}
					}
	 			}
	 			
	 			// 处理粘包
				if (_buffer.length < _offset) {
					throw new IndexOutOfBoundsException("Not enough data.");
					
				} else if (_buffer.length == _offset) {
					_offset = 0;
					_buffer = null;
					
//					// DEBUG					
//					if ( pipeline.size() > 1 ) {
//						LOGGER.warn( JavaUtils.toPrintableString( buffer ) );
//					}
					
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
	
	/**
	 * bulk命令使用 (把value长度转换出来)
	 * @param length
	 * @return
	 * @throws IndexOutOfBoundsException
	 * @throws RedisRequestUnknowException
	 */
	private int getBulkValueLength(byte[] length) throws IndexOutOfBoundsException, RedisRequestUnknowException {

		long v = 0;
		boolean isNeg = false;
		int offset = 0;
		byte b = length[offset];
		if (b == '-') {
			isNeg = true;
			offset++;
			if (offset >= length.length) {
				throw new IndexOutOfBoundsException("Not enough data.");
			}
		}

		while (offset < length.length) {
			b = length[offset];
			int value = b - '0';
			if (value >= 0 && value < 10) {
				v *= 10;
				v += value;
			} else {
				throw new RedisRequestUnknowException("Invalid character in integer");
			}

			offset++;
		}

		v = (isNeg ? -v : v);
		return (int) v;
	}
	
	
	// 增加字节流
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
	    return result;
	} 

	/**
	 * 跳过数据开始的空格
	 */
	private void skipBytes() {
		while (true) {			
			if ( _offset >= _buffer.length ) {
				  throw new IndexOutOfBoundsException("Not enough data.");
			}	
			
			byte b = _buffer[ _offset ];
			if (b == ' ') {
				_offset++;
			} else {
				break;
			}
		}
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
		
		RedisRequestDecoderV4 decoder = new RedisRequestDecoderV4();
		long t = System.currentTimeMillis();
	    for(int j = 0; j < 10000000; j++) {	    	
	    	try {	    		
	    		byte[] buff = "*2\r\n$3\r\nGET\r\n$2\r\naa\r\n".getBytes();
	    		buff = "  *1\r\n$5\r\nMULTI\r\n*2\r\n$4\r\nLLEN\r\n$6\r\ncelery\r\n*1\r\n$4\r\nEXEC\r\n".getBytes();
//	    		buff = "*2\r\n$4\r\nKEYS\r\n$1\r\\*\r\n".getBytes();
//	    		buff = "QUIT\r\n".getBytes();
//	    		buff = "*1\r\n$4\r\nPING\r\n".getBytes();
//	    		buff = "set test 4\r\ntest\r\n".getBytes();
//	    		buff = "set test testxxx\r\n".getBytes();
	    		
	    		byte[] buff1 = new byte[ buff.length - 8 ];
	    		byte[] buff2 = new byte[ buff.length - buff1.length ];
	    		System.arraycopy(buff, 0, buff1, 0, buff1.length);
	    		System.arraycopy(buff, buff1.length, buff2, 0, buff2.length);
	    		
	    		//byte[] buff1 = buff;
	    		//byte[] buff2  = new byte[buff.length + buff1.length];
	    		//System.arraycopy(buff, 0, buff2, 0, buff.length);
	    		//System.arraycopy(buff1, 0, buff2, buff.length, buff1.length);
	    		
	    		
	    		
	    		long t1 = System.currentTimeMillis();
				List<RedisRequest> reqs = decoder.decode( buff );								
//				reqs = decoder.decode( buff2  );								
				long t2 = System.currentTimeMillis();
		    	int diff = (int)(t2-t1);
		    	if ( diff > 1) {
		    		System.out.println(" decode diff=" + diff + ", req=" + reqs.toString() );
		    	}
				
			} catch (RedisRequestUnknowException e) {
				e.printStackTrace();
			}
	    }  
	    System.out.println(System.currentTimeMillis() - t);
	}
}