package com.feeyo.net.codec;

import java.util.ArrayList;
import java.util.List;

public class RedisResponsePipelineDecoder {

	private byte[] _buffer;
	private int _offset;
	private List<Integer> index = new ArrayList<Integer>();
	
	// 应答
	public class PipelineResponse {
		
		public static final byte ERR = 0;	// 不全
		public static final byte OK = 1;
		
		private byte status;
		private int count;
		private byte[][] resps;
		
		public PipelineResponse (byte status, int count, byte[][] resps) {
			this.status = status;
			this.count = count;
			this.resps = resps;
		}
		
		public int getCount() {
			return count;
		}
		
		public byte[][] getResps() {
			return resps;
		}
		
		public boolean isOK () {
			return status == OK;
		}
	}

	/**
	 * 解析返回 数量、内容
	 */
	public PipelineResponse parse(byte[] buffer) {
		int result = 0;
		append(buffer);
		try {

			for (;;) {

				// 至少4字节 :1\r\n
				if (bytesRemaining() < 4) {
					return new PipelineResponse(PipelineResponse.ERR, 0, null);
				}

				byte type = _buffer[_offset++];
				switch (type) {
				case '*': // 数组(Array), 以 "*" 开头,表示消息体总共有多少行（不包括当前行）, "*" 是具体行数
				case '+': // 正确, 表示正确的状态信息, "+" 后就是具体信息
				case '-': // 错误, 表示错误的状态信息, "-" 后就是具体信息
				case ':': // 整数, 以 ":" 开头, 返回
				case '$': // 批量字符串, 以 "$"
					// 解析，只要不抛出异常，就是完整的一条数据返回
					parseResponse(type);
					result ++;
					index.add(_offset);
				}

				if (_buffer.length < _offset) {
					throw new IndexOutOfBoundsException("Not enough data.");

				} else if (_buffer.length == _offset) {
					_offset = 0;
					return new PipelineResponse(PipelineResponse.OK, result, getResponses());
				}
			}

		} catch (IndexOutOfBoundsException e1) {
			// 捕获这个错误（没有足够的数据），等待下一个数据包
			_offset = 0;
			index.clear();
			return new PipelineResponse(PipelineResponse.ERR, 0, null);
		}
	}

	private void parseResponse(byte type) {
		int end, offset;

		if (type == '+' || type == '-' || type == ':') {

			offset = _offset;
			end = readCRLFOffset(); // 分隔符 \r\n
			_offset = end; // 调整偏移值

			if (end > _buffer.length) {
				_offset = offset;
				throw new IndexOutOfBoundsException("Wait for more data.");
			}
		} else if (type == '$') {

			offset = _offset;

			// 大小为 -1的数据包被认为是 NULL
			int packetSize = readInt();
			if (packetSize == -1) {
				end = _offset;
				return; // 此处不减
			}

			end = _offset + packetSize + 2; // offset + data + \r\n
			_offset = end; // 调整偏移值

			if (end > _buffer.length) {
				_offset = offset - 1;
				throw new IndexOutOfBoundsException("Wait for more data.");
			}
		} else if (type == '*') {

			offset = _offset;

			// 大小为 -1的数据包被认为是 NULL
			int packetSize = readInt();
			if (packetSize == -1) {
				end = _offset;
				return; // 此处不减
			}

			if (packetSize > bytesRemaining()) {
				_offset = offset - 1;
				throw new IndexOutOfBoundsException("Wait for more data.");
			}

			// 此处多增加一长度，用于存储 *packetSize\r\n
			end = _offset;

			byte ntype;
			for (int i = 1; i <= packetSize; i++) {
				if (_offset + 1 >= _buffer.length) {
					throw new IndexOutOfBoundsException("Wait for more data.");
				}

				ntype = _buffer[_offset++];
				parseResponse(ntype);
			}
		}
	}

	private int bytesRemaining() {
		return (_buffer.length - _offset) < 0 ? 0 : (_buffer.length - _offset);
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

	private int readCRLFOffset() throws IndexOutOfBoundsException {
		int offset = _offset;

		if (offset + 1 >= _buffer.length) {
			throw new IndexOutOfBoundsException("Not enough data.");
		}

		while (_buffer[offset] != '\r' && _buffer[offset + 1] != '\n') {
			offset++;
			if (offset + 1 == _buffer.length) {
				throw new IndexOutOfBoundsException("didn't see LF after NL reading multi bulk count (" + offset
						+ " => " + _buffer.length + ", " + _offset + ")");
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
		byte[] largeBuffer = new byte[_buffer.length + newBuffer.length];
		System.arraycopy(_buffer, 0, largeBuffer, 0, _buffer.length);
		System.arraycopy(newBuffer, 0, largeBuffer, _buffer.length, newBuffer.length);

		_buffer = largeBuffer;
		_offset = 0;
		index.clear();
	}

	// 获取分段后的response
	private byte[][] getResponses() {
		byte[][] result = new byte[index.size()][];
		
		for (int i = 0; i < index.size(); i++) {
			int start;
			if (i == 0) {
				start = 0;
			} else {
				start = index.get(i - 1); 
			}
			int end = index.get(i);
			
			result[i] = new byte[end - start];
			System.arraycopy(_buffer, start, result[i], 0, result[i].length);
		}
		
		index.clear();
		_buffer = null;
		return result;
	}

	public static void main(String[] args) {

		byte[] buffer = "+PONG \r\n".getBytes();
		buffer = "-ERR Not implemented\r\n".getBytes();
		// buffer = "*-1\r\n".getBytes();
		// buffer = "$12\r\n1cccccccccc9\r\n".getBytes();
		// buffer = "$-1\r\n".getBytes();
		buffer = "*2\r\n$7\r\npre1_bb\r\n$7\r\npre1_aa\r\n".getBytes();

		// buffer =
		// "*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_aa\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_zz\r\n:2\r\n".getBytes();

		buffer = "xxx$5\r\nxxxxx\r\n$5\r\nxxxxx\r\n".getBytes();

		byte[] buffer1 = new byte[buffer.length - 4];
		byte[] buffer2 = new byte[buffer.length - buffer1.length];

		System.arraycopy(buffer, 0, buffer1, 0, buffer1.length);
		System.arraycopy(buffer, buffer1.length, buffer2, 0, buffer2.length);

		RedisResponsePipelineDecoder decoder = new RedisResponsePipelineDecoder();
		// List<RedisResponseV3> resps = decoder.decode(buffer);

		PipelineResponse result  = decoder.parse(buffer1);
		System.out.println( result );
	}

}
