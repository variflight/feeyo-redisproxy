package com.feeyo.net.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultArrayBucket extends AbstractBucket {
	
	private final ConcurrentLinkedQueue<ByteBuffer>[] queueArray;
	
	private final AtomicInteger pollIdx = new AtomicInteger(0);
	private final AtomicInteger offerIdx = new AtomicInteger(0);

	@SuppressWarnings("unchecked")
	public DefaultArrayBucket(BucketBufferPool pool, int chunkSize, int count, boolean isExpand) {
		super(pool, chunkSize, count, isExpand);

		this.queueArray = new ConcurrentLinkedQueue[16];
		for (int i = 0; i < queueArray.length; i++) {
			this.queueArray[i] = new ConcurrentLinkedQueue<ByteBuffer>();
		}
		
		// 初始化
		for (int j = 0; j < count; j++) {
			queueOffer(ByteBuffer.allocateDirect(chunkSize));
			pool.getUsedBufferSize().addAndGet(chunkSize);
		}
	}

	@Override
	public int compareTo(AbstractBucket o) {
		if (this.getChunkSize() > o.getChunkSize()) {
			return 1;
		} else if (this.getChunkSize() < o.getChunkSize()) {
			return -1;
		}
		return 0;
	}

	@Override
	protected boolean queueOffer(ByteBuffer buffer) {
		ConcurrentLinkedQueue<ByteBuffer> queue = this.queueArray[ getNextIndex(offerIdx) ];
		return queue.offer( buffer );
	}

	@Override
	protected ByteBuffer queuePoll() {
		ConcurrentLinkedQueue<ByteBuffer> queue = this.queueArray[ getNextIndex(pollIdx) ];
		return queue.poll();
	}

	@Override
	protected void containerClear() {
		for (ConcurrentLinkedQueue<ByteBuffer> cq : queueArray) {
			cq.clear();
		}
	}

	@Override
	public int getQueueSize() {
		int size = 0;
		for (ConcurrentLinkedQueue<ByteBuffer> cq : queueArray) {
			size = size + cq.size();
		}
		return size;
	}
	  
	private final int getNextIndex(AtomicInteger atomic) {
		for (;;) {
			int current = atomic.get();
			int next = current >= 15 ? 0 : current + 1;
			if (atomic.compareAndSet(current, next))
				return current;
		}
	}
}
