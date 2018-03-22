package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.feeyo.redis.nio.buffer.BufferPool;

public class SegmentByteBufferBucket extends AbstractByteBufferBucket {
	
	private final ConcurrentLinkedQueue<ByteBuffer>[] buffers;
	private final static int buffersSize = 16;
	private final AtomicInteger allocateIndex = new AtomicInteger(0);
	private final AtomicInteger recycleIndex = new AtomicInteger(0);
	
	private final ThreadLocalBufferPool localBufferPool;
	private int threadLocalCount;
	private final int localBufThreadPrexLength;

	
	@SuppressWarnings("unchecked")
	public SegmentByteBufferBucket(ByteBufferBucketPool pool, int chunkSize, int count, boolean isExpand, int threadLocalPercent) {
		super(pool, chunkSize, count, isExpand, threadLocalPercent);

		this.buffers = new ConcurrentLinkedQueue[buffersSize];
		for (int i = 0; i < buffers.length; i++) {
			this.buffers[i] = new ConcurrentLinkedQueue<ByteBuffer>();
		}
		
		// 初始化
		for (int j = 0; j < count; j++) {
			queueOffer(ByteBuffer.allocateDirect(chunkSize));
			pool.getUsedBufferSize().addAndGet(chunkSize);
		}
		
		this.localBufThreadPrexLength = BufferPool.LOCAL_BUF_THREAD_PREX.length();
		this.threadLocalCount = count * threadLocalPercent / 100;
		
		this.localBufferPool = new ThreadLocalBufferPool(threadLocalCount);
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
		
		if (isLocalCacheThread()) {
			BufferQueue localQueue = localBufferPool.get();
			if (localQueue.snapshotSize() < 100) {
				localQueue.put(buffer);
			} else {
				// recyle 3/4 thread local buffer
				Collection<ByteBuffer> buffers = localQueue.removeItems(threadLocalCount * 1 / 4);
				for (ByteBuffer buf : buffers) {
					this.buffers[getIndex(recycleIndex)].offer( buf );
				}
				this.buffers[getIndex(recycleIndex)].offer( buffer );
			}
			return true;
		}
		
		return this.buffers[getIndex(recycleIndex)].offer( buffer );
	}

	@Override
	protected ByteBuffer queuePoll() {
		
		// 从线程的二级缓存 获取
		if ( isLocalCacheThread() ) {
			// allocate from threadlocal
			ByteBuffer bb = localBufferPool.get().poll();
			if (bb != null) {
				return bb;
			}
		}
		
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
	
	private final boolean isLocalCacheThread() {
		final String thname = Thread.currentThread().getName();
		return (thname.length() < localBufThreadPrexLength) ? false
				: (thname.charAt(0) == '$' && thname.charAt(1) == '_');
	}

}
