package com.feeyo.redis.nio.buffer.bucket;

public class ThreadLocalBufferPool extends ThreadLocal<BufferQueue> {
	private final long size;

	public ThreadLocalBufferPool(long size) {
		this.size = size;
	}

	protected synchronized BufferQueue initialValue() {
		return new BufferQueue(size);
	}
	
	protected synchronized long getSize() {
		return this.size;
	}
}
