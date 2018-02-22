package com.feeyo.redis.engine.codec;

import java.nio.ByteBuffer;

import com.feeyo.redis.nio.NetSystem;

public class AbstractDecoder {
	protected ByteBuffer _buffer;
	protected int _offset;
	
	// 增加字节流
	protected void append(ByteBuffer newBuffer) {

		if (newBuffer == null) {
			return;
		}

		if (_buffer == null) {
			_buffer = newBuffer;
			return;
		}

		_buffer = margeByteBuffer(_buffer, newBuffer);
		_offset = 0;
	}

	protected ByteBuffer margeByteBuffer(ByteBuffer a, ByteBuffer b) {

		if (a.hasRemaining() && a.capacity() - a.position() >= b.position()) {
			b.flip();
			a.put(b);
			recycleByteBuffer(b);
			return a;
		} else {
			int size = a.position() + b.position();
			ByteBuffer buf = allocateByteBuffer(size);
			a.flip();
			buf.put(a);
			recycleByteBuffer(a);
			b.flip();
			buf.put(b);
			recycleByteBuffer(b);
			return buf;
		}
	}
	
	/**
	 * 从bytebuffer读取数据
	 * @param offset
	 * @param length
	 * @return
	 */
	protected byte[] getBytes(int offset, int length) {
		byte[] result = new byte[length];
		for (int i = 0; i < length; i++) {
			result[i] = _buffer.get(offset + i);
		}
		return result;
	}
	
	protected int readInt() throws IndexOutOfBoundsException {

		long size = 0;
		boolean isNeg = false;

		if (_offset >= _buffer.position()) {
			throw new IndexOutOfBoundsException("Not enough data.");
		}

		byte b = _buffer.get(_offset);
		while (b != '\r') {
			if (b == '-') {
				isNeg = true;
			} else {
				size = size * 10 + b - '0';
			}
			_offset++;

			if (_offset >= _buffer.position()) {
				throw new IndexOutOfBoundsException("Not enough data.");
			}
			b = _buffer.get(_offset);
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
	
	protected int readCRLFOffset() throws IndexOutOfBoundsException {
		int offset = _offset;

		if (offset + 1 >= _buffer.position()) {
			throw new IndexOutOfBoundsException("Not enough data.");
		}

		while (_buffer.get(offset) != '\r' && _buffer.get(offset + 1) != '\n') {
			offset++;
			if (offset + 1 == _buffer.position()) {
				throw new IndexOutOfBoundsException("didn't see LF after NL reading multi bulk count (" + offset
						+ " => " + _buffer.position() + ", " + _offset + ")");
			}
		}
		offset++;
		offset++;
		return offset;
	}
	
	protected int bytesRemaining() {
	    return (_buffer.position() - _offset) < 0 ? 0 : (_buffer.position() - _offset);
	}
	
	protected void recycleByteBuffer(ByteBuffer buf) {
		NetSystem.getInstance().getBufferPool().recycle(buf);
	}
	
	protected ByteBuffer allocateByteBuffer(int size) {
		return NetSystem.getInstance().getBufferPool().allocate(size);
	}
}
