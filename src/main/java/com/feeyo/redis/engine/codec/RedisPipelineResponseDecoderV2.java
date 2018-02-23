package com.feeyo.redis.engine.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RedisPipelineResponseDecoderV2 extends AbstractDecoder {

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
	public PipelineResponse parse(ByteBuffer buffer) {
		int result = 0;
		append(buffer);
		try {

			for (;;) {

				// 至少4字节 :1\r\n
				if (bytesRemaining() < 4) {
					return new PipelineResponse(PipelineResponse.ERR, 0, null);
				}

				byte type = _buffer.get(_offset++);
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

				if (_buffer.position() < _offset) {
					throw new IndexOutOfBoundsException("Not enough data.");

				} else if (_buffer.position() == _offset) {
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

			if (end > _buffer.position()) {
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

			if (end > _buffer.position()) {
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
				if (_offset + 1 >= _buffer.position()) {
					throw new IndexOutOfBoundsException("Wait for more data.");
				}

				ntype = _buffer.get(_offset++);
				parseResponse(ntype);
			}
		}
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

			result[i] = getBytes(start, end - start);
		}

		index.clear();
		
		cleanup();
		
		return result;
	}
	

	public static void main(String[] args) {
		
	}

}
