package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is thread safe.
 *  
 * @author zhuam
 *
 */
public class ByteBufferBucket implements Comparable<ByteBufferBucket> {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ByteBufferBucket.class );

	private ByteBufferBucketPool bufferPool;
	
	// TODO: ConcurrentLinkedQueue
	// https://bugs.openjdk.java.net/browse/JDK-8137185
	private final ConcurrentLinkedDeque<ByteBuffer> buffers = new ConcurrentLinkedDeque<ByteBuffer>();
	
	private Object _lock = new Object();
	
	private int count = 0;
	private final int chunkSize;
	private long _shared = 0;

	public ByteBufferBucket(ByteBufferBucketPool pool,int chunkSize) {
		this(pool, chunkSize, 0);
	}
	
	public ByteBufferBucket(ByteBufferBucketPool pool, int chunkSize, int count) {
		this.bufferPool = pool;
		this.chunkSize = chunkSize;
		this.count = count;
		
		// 初始化
		for(int j = 0; j < count; j++ ) {
			queueOffer( ByteBuffer.allocateDirect(chunkSize) );
			pool.getUsedBufferSize().addAndGet( chunkSize );
		}
	}
	
	//
	public ByteBuffer allocate() {
		
		ByteBuffer bb = queuePoll();
		if (bb != null) {
			// Clear sets limit == capacity. Position == 0.
			bb.clear();
			return bb;
		}
		
		// 桶内内存块不足，创建新的块
		synchronized ( _lock ) {
			// 容量阀值
			long used = bufferPool.getUsedBufferSize().get();
			
			if ( ( used + chunkSize ) < bufferPool.getMaxBufferSize()) { 
				bb = ByteBuffer.allocateDirect( chunkSize );
				bufferPool.getUsedBufferSize().addAndGet( chunkSize );
			} 
			count++;
			return bb;
		}
		
	}

	public void recycle(ByteBuffer buf) {
	
		if ( buf.capacity() != this.chunkSize ) {
			LOGGER.warn("Trying to put a buffer, not created by this bucket! Will be just ignored");
			return;
		}
		
		buf.clear();
		queueOffer( buf );
		_shared++;
	}

	@SuppressWarnings("restriction")
	public synchronized void clear() {
		ByteBuffer buffer = null;
		while( (buffer = queuePoll()) != null) {
			if ( buffer.isDirect()) {
				((sun.nio.ch.DirectBuffer)buffer).cleaner().clean();
			}
		}
		buffers.clear();
	}

	public int getCount() {
		return count;
	}
	
	public long getShared() {
		return _shared;
	}
	
    private void queueOffer(ByteBuffer buffer)
    {
        this.buffers.offerFirst(buffer);
    }

    private ByteBuffer queuePoll()
    {
        return this.buffers.poll();
    }

	
	public int getQueueSize() {
		return this.buffers.size();
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
		//return super.hashCode();
		return this.chunkSize;
	}
	
	@Override
	public int compareTo(ByteBufferBucket o) {
		if (this.chunkSize > o.chunkSize) {
			return 1;
		} else if (this.chunkSize < o.chunkSize) {
			return -1;
		}
		return 0;
	}

}