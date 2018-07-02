package com.feeyo.net.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.buffer.bucket.ref.ByteBufferReference;

public abstract class AbstractBucket implements Comparable<AbstractBucket> {
	
	private static Logger LOGGER = LoggerFactory.getLogger(AbstractBucket.class);

	private BucketBufferPool bufferPool;

	private final ConcurrentHashMap<Long, ByteBufferReference> references;

	private Object _lock = new Object();

	private final AtomicInteger count;
	private final AtomicInteger usedCount;

	private final int chunkSize;
	private long _shared = 0;
	private boolean isExpand = false;
	

	public AbstractBucket(BucketBufferPool pool, int chunkSize) {
		this(pool, chunkSize, 0, false);
	}

	public AbstractBucket(BucketBufferPool pool, int chunkSize, int count, boolean isExpand) {
		this.bufferPool = pool;
		this.chunkSize = chunkSize;

		this.count = new AtomicInteger(count);
		this.usedCount = new AtomicInteger(0);
		this.isExpand = isExpand;

		this.references = new ConcurrentHashMap<Long, ByteBufferReference>(count, 0.2F, 32);
	}

	protected abstract boolean queueOffer(ByteBuffer buffer);

	protected abstract ByteBuffer queuePoll();

	protected abstract void containerClear();

	public abstract int getQueueSize();

	@SuppressWarnings("restriction")
	public ByteBuffer allocate() {

		ByteBuffer bb = queuePoll();
		if (bb != null) {
			try {
				long address = ((sun.nio.ch.DirectBuffer) bb).address();
				ByteBufferReference reference = references.get(address);
				if (reference == null) {
					reference = new ByteBufferReference(address, bb);
					references.put(address, reference);
				}

				// 检测
				if (reference.isItAllocatable()) {
					this.usedCount.incrementAndGet();

					// Clear sets limit == capacity. Position == 0.
					bb.clear();
					return bb;

				} else {
					return null;
				}
			} catch (Exception e) {
				LOGGER.error("allocate err", e);
				return bb;
			}
		}

		// 是否支持自动扩展
		if (isExpand) {

			// 桶内内存块不足，创建新的块
			synchronized (_lock) {

				// 容量阀值
				long poolUsed = bufferPool.getUsedBufferSize().get();
				if ((poolUsed + chunkSize) < bufferPool.getMaxBufferSize()) {
					bb = ByteBuffer.allocateDirect(chunkSize);
					this.count.incrementAndGet();
					this.usedCount.incrementAndGet();
					bufferPool.getUsedBufferSize().addAndGet(chunkSize);
				}

				return bb;
			}
		}

		return null;

	}
	
	@SuppressWarnings("restriction")
	public void recycle(ByteBuffer buf) {

		if (buf.capacity() != this.chunkSize) {
			LOGGER.warn("Trying to put a buffer, not created by this bucket! Will be just ignored");
			return;
		}

		try {
			long address = ((sun.nio.ch.DirectBuffer) buf).address();
			ByteBufferReference reference = references.get(address);
			if (reference == null) {
				reference = new ByteBufferReference(address, buf);
				references.put(address, reference);

			} else {
				// 如果不能回收，则返回
				if (!reference.isItRecyclable()) {
					return;
				}
			}

		} catch (Exception e) {
			LOGGER.error("recycle err", e);
		}

		usedCount.decrementAndGet();

		buf.clear();
		queueOffer(buf);
		_shared++;
	}

	/**
	 * 释放超时的 buffer
	 */
	public void releaseTimeoutBuffer() {

		List<Long> timeoutAddrs = null;

		for (Entry<Long, ByteBufferReference> entry : references.entrySet()) {
			ByteBufferReference ref = entry.getValue();
			if (ref.isTimeout()) {

				if (timeoutAddrs == null)
					timeoutAddrs = new ArrayList<Long>();

				timeoutAddrs.add(ref.getAddress());
			}
		}

		//
		if (timeoutAddrs != null) {
			for (long addr : timeoutAddrs) {
				boolean isRemoved = false;
				ByteBufferReference addrRef = references.remove(addr);
				if (addrRef != null) {

					ByteBuffer oldBuffer = addrRef.getByteBuffer();
					oldBuffer.clear();

					isRemoved = queueOffer(oldBuffer);
					_shared++;
					usedCount.decrementAndGet();
				}

				//
				LOGGER.warn("##buffer reference release addr:{}, isRemoved:{}", addrRef, isRemoved);
			}
		}
	}

	@SuppressWarnings("restriction")
	public synchronized void clear() {
		ByteBuffer buffer = null;
		while ((buffer = queuePoll()) != null) {
			if (buffer.isDirect()) {
				((sun.nio.ch.DirectBuffer) buffer).cleaner().clean();
			}
		}

		containerClear();
	}

	public int getCount() {
		return this.count.get();
	}

	public int getUsedCount() {
		return this.usedCount.get();
	}

	public long getShared() {
		return _shared;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	@Override
	public String toString() {
		return String.format("Bucket@%x{%d/%d}", hashCode(), count, chunkSize);
	}

	@Override
	public int hashCode() {
		// return super.hashCode();
		return this.chunkSize;
	}
	
}
