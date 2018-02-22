package com.feeyo.redis.engine.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RedisRequestDecoderV6 extends AbstractDecoder {
	
	private enum State {
		READ_SKIP, 			// 跳过空格
		READ_INIT, 			// 开始
		READ_ARG_COUNT, 	// 读取参数数量(新协议)
		READ_ARG_LENGTH, 	// 读取参数长度(新协议)
		READ_ARG,  			// 读取参数(新协议)
		READ_END            // 结束
	}

	private RedisRequest request = null;
	private State state = State.READ_SKIP;	
	
	public void reset() {
		state = State.READ_SKIP;
		_offset = 0;
		recycleByteBuffer(_buffer);
		_buffer = null;
	}

	public List<RedisRequest> decode(ByteBuffer buffer) throws RedisRequestUnknowException {
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
					if (_offset >= _buffer.position() || ( argCount != 0 && argCount == argIndex + 1)) {
						state = State.READ_END;
						break;
					}
					if (_buffer.get(_offset) == '*') {
						_offset++;
						state = State.READ_ARG_COUNT;
					} else if (_buffer.get(_offset) == '$') {
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
					byte[] buf = getBytes(_offset, argLength);
					request.getArgs()[argIndex] = buf;
					_offset += argLength;
					_offset++;
					_offset++;
					
					this.state = State.READ_INIT;
					break;
				}
				case READ_END: {
					// 处理粘包
					if (_buffer.position() < _offset) {
						throw new IndexOutOfBoundsException("Not enough data.");
					} else if (_buffer.position() == _offset) {
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
			if ( _offset >= _buffer.position() ) {
				  throw new IndexOutOfBoundsException("Not enough data.");
			}	
			
			byte b = _buffer.get(_offset);
			if (b == '*') {
				break;
			}
			_offset++;
		}
	}
	
	public static void main(String[] args) {
		
	}
}
