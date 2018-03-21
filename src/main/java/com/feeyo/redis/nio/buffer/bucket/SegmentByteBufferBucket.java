package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentByteBufferBucket extends AbstractByteBufferBucket {
	
	private final ConcurrentLinkedQueue<ByteBuffer>[] buffers;
	private final static int buffersSize = 16;
	private final AtomicInteger allocateIndex = new AtomicInteger(0);
	private final AtomicInteger recycleIndex = new AtomicInteger(0);
	
	@SuppressWarnings("unchecked")
	public SegmentByteBufferBucket(ByteBufferBucketPool pool, int chunkSize, int count, boolean isExpand) {
		super(pool, chunkSize, count, isExpand);

		this.buffers = new ConcurrentLinkedQueue[buffersSize];
		for (int i = 0; i < buffers.length; i++) {
			this.buffers[i] = new ConcurrentLinkedQueue<ByteBuffer>();
		}
		
		// 初始化
		for (int j = 0; j < count; j++) {
			queueOffer(ByteBuffer.allocateDirect(chunkSize));
			pool.getUsedBufferSize().addAndGet(chunkSize);
		}
	}

	@Override
	public int compareTo(ByteBufferBucket o) {
		if (this.getChunkSize() > o.getChunkSize()) {
			return 1;
		} else if (this.getChunkSize() < o.getChunkSize()) {
			return -1;
		}
		return 0;
	}

	@Override
	protected boolean queueOffer(ByteBuffer buffer) {
		return this.buffers[getIndex(recycleIndex)].offer( buffer );
	}

	@Override
	protected ByteBuffer queuePoll() {
		return (ByteBuffer) this.buffers[getIndex(allocateIndex)].poll();
	}

	@Override
	protected void containerClear() {
		for (ConcurrentLinkedQueue<ByteBuffer> cq : buffers) {
			cq.clear();
		}
	}

	@Override
	public int getQueueSize() {
		int size = 0;
		for (ConcurrentLinkedQueue<ByteBuffer> cq : buffers) {
			size = size + cq.size();
		}
		return size;
	}
	  
	private final int getIndex(AtomicInteger ai) {
		for (;;) {
			int current = ai.get();
			int next = current >= 15 ? 0 : current + 1;
			if (ai.compareAndSet(current, next))
				return current;
		}
	}

}
