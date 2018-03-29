package com.feeyo.redis.net.codec;

import java.util.ArrayList;
import java.util.List;

public class RedisRequestDecoder {
	
	private enum State {
		READ_SKIP, 			// 跳过空格
		READ_INIT, 			// 开始
		READ_ARG_COUNT, 	// 读取参数数量(新协议)
		READ_ARG_LENGTH, 	// 读取参数长度(新协议)
		READ_ARG,  			// 读取参数(新协议)
		READ_END            // 结束
	}

	private RedisRequest request = null;
	private byte[] _buffer;
	private int _offset;
	private State state = State.READ_SKIP;	
	
	public void reset() {
		state = State.READ_SKIP;
		_offset = 0;
		_buffer = null;
	}

	public List<RedisRequest> decode(byte[] buffer) throws RedisRequestUnknowException {
		append(buffer);
		// pipeline
		List<RedisRequest> pipeline = new ArrayList<RedisRequest>();
		
		try {
			// 读取到的参数索引
			int argIndex = -1;
			// 参数的数量
			int argCount = 0;
			// 参数的长度
			int argLength = 0;
			
			decode : for(;;) {
				switch (state) {
				case READ_SKIP: {
					skipBytes();
					request = new RedisRequest();
					state = State.READ_INIT;
					break;
				}
				case READ_INIT: {
					if (_offset >= _buffer.length || ( argCount != 0 && argCount == argIndex + 1)) {
						state = State.READ_END;
						break;
					}
					if (_buffer[_offset] == '*') {
						_offset++;
						state = State.READ_ARG_COUNT;
					} else if (_buffer[_offset] == '$') {
						_offset++;
						state = State.READ_ARG_LENGTH;
					} 
					break;
				}
				case READ_ARG_COUNT: {
					argCount = readInt();
					byte[][] args = new byte[ argCount ][];
					request.setArgs( args );
					this.state = State.READ_INIT;
					break;
				}
				case READ_ARG_LENGTH: {
					argLength = readInt();
					argIndex++;
					this.state = State.READ_ARG;
					break;
				}
				case READ_ARG: {
					byte[] buf = new byte[argLength];
					System.arraycopy(_buffer, _offset, buf, 0, argLength);
					request.getArgs()[argIndex] = buf;
					_offset += argLength;
					_offset++;
					_offset++;
					
					this.state = State.READ_INIT;
					break;
				}
				case READ_END: {
					// 处理粘包
					if (_buffer.length < _offset) {
						throw new IndexOutOfBoundsException("Not enough data.");
					} else if (_buffer.length == _offset) {
						if (argCount == argIndex + 1) {
							pipeline.add(request);
							reset();
							// 整包解析完成
							break decode;
							
						// 断包（目前异步读取到的都是整包数据）
						} else {
							state = State.READ_SKIP;
							_offset = 0;
							return null;
						}
					} else {
						argIndex = -1;
						argCount = 0;
						argLength = 0;
						pipeline.add(request);
						this.state = State.READ_SKIP;
					}
				}
				break;
				default:
					throw new RedisRequestUnknowException("Unknown state: " + state);
				}
			}
		} catch (IndexOutOfBoundsException e) {
			state = State.READ_SKIP;
			_offset = 0;
 			return null;
		}
		
		return pipeline;
	}
	
	/**
	 * 如果第一个字符不是*则skip直到遇到*
	 */
	private void skipBytes() {
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

		if (_offset >= _buffer.length) {
			throw new IndexOutOfBoundsException("Not enough data.");
		}

		byte b = _buffer[_offset];
		while (b != '\r') {
			if (b == '-') {
				isNeg = true;
			} else {
				size = size * 10 + b - '0';
			}
			_offset++;

			if (_offset >= _buffer.length) {
				throw new IndexOutOfBoundsException("Not enough data.");
			}
			b = _buffer[_offset];
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
		return (int) size;
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
	
	public static void main(String[] args) {
		RedisRequestDecoder decoder = new RedisRequestDecoder();
		long t = System.currentTimeMillis();
	    for(int j = 0; j < 10000000; j++) {	    	
	    	try {	    		
	    		byte[] buff = "*2\r\n$3\r\nGET\r\n$2\r\naa\r\n".getBytes();
	    		buff = "  *1\r\n$5\r\nMULTI\r\n*2\r\n$4\r\nLLEN\r\n$6\r\ncelery\r\n*1\r\n$4\r\nEXEC\r\n".getBytes();
	    		buff = "*2\r\n$4\r\nKEYS\r\n$1\r\\*\r\n".getBytes();
//	    		buff = "QUIT\r\n".getBytes();
//	    		buff = "*1\r\n$4\r\nPING\r\n".getBytes();
//	    		buff = "set test 4\r\ntest\r\n".getBytes();
//	    		buff = "set test testxxx\r\nset test te\r\n".getBytes();
	    		
	    		byte[] buff1 = new byte[ buff.length - 8 ];
	    		byte[] buff2 = new byte[ buff.length - buff1.length ];
	    		System.arraycopy(buff, 0, buff1, 0, buff1.length);
	    		System.arraycopy(buff, buff1.length, buff2, 0, buff2.length);
	    		
//	    		byte[] buff1 = buff;
//	    		byte[] buff2  = new byte[buff.length + buff1.length];
//	    		System.arraycopy(buff, 0, buff2, 0, buff.length);
//	    		System.arraycopy(buff1, 0, buff2, buff.length, buff1.length);
	    		
	    		
	    		
	    		long t1 = System.currentTimeMillis();
				List<RedisRequest> reqs = decoder.decode( buff1 );								
				reqs = decoder.decode( buff2  );								
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
