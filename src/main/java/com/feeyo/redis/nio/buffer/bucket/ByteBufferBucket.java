package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
	
	private final ConcurrentLinkedQueue<ByteBuffer> buffers = new ConcurrentLinkedQueue<ByteBuffer>();
	
	private final ConcurrentHashMap<Long, ByteBufferReference> bufferReferencMap;
	
	private Object _lock = new Object();
	
	private final AtomicInteger count;
	private final AtomicInteger usedCount;
	
	private final int chunkSize;
	private long _shared = 0;

	public ByteBufferBucket(ByteBufferBucketPool pool,int chunkSize) {
		this(pool, chunkSize, 0);
	}
	
	public ByteBufferBucket(ByteBufferBucketPool pool, int chunkSize, int count) {
		
		this.bufferPool = pool;
		this.chunkSize = chunkSize;
		
		this.count = new AtomicInteger(count);
		this.usedCount = new AtomicInteger(0);
		
		this.bufferReferencMap = new ConcurrentHashMap<Long, ByteBufferReference>(  (int)(count * 1.6) );
		
		// 初始化
		for(int j = 0; j < count; j++ ) {
			queueOffer( ByteBuffer.allocateDirect(chunkSize) );
			pool.getUsedBufferSize().addAndGet( chunkSize );
		}
	}
	
	//
	@SuppressWarnings("restriction")
	public ByteBuffer allocate() {
		
		ByteBuffer bb = queuePoll();
		if (bb != null) {
			try {
				long address = ((sun.nio.ch.DirectBuffer) bb).address();
				ByteBufferReference reference = bufferReferencMap.get( address );
				if (reference == null) {
					reference = new ByteBufferReference( address, bb );
					bufferReferencMap.put(address, reference);
				}
				
				// 检测
				if ( reference.isItAllocatable() ) {
					
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
		
		// 桶内内存块不足，创建新的块
		synchronized ( _lock ) {
			
			// 容量阀值
			long poolUsed = bufferPool.getUsedBufferSize().get();
			if ( ( poolUsed + chunkSize ) < bufferPool.getMaxBufferSize()) { 
				bb = ByteBuffer.allocateDirect( chunkSize );

				this.count.incrementAndGet();
				this.usedCount.incrementAndGet();
				bufferPool.getUsedBufferSize().addAndGet( chunkSize );
			} 
			
			return bb;
		}
		
	}

	@SuppressWarnings("restriction")
	public void recycle(ByteBuffer buf) {
	
		if ( buf.capacity() != this.chunkSize ) {
			LOGGER.warn("Trying to put a buffer, not created by this bucket! Will be just ignored");
			return;
		}
		
		try {
			long address = ((sun.nio.ch.DirectBuffer) buf).address();
			ByteBufferReference reference = bufferReferencMap.get(address);
			if ( reference == null ) {
				 reference = new ByteBufferReference( address, buf );
				 bufferReferencMap.put(address, reference);
				 
			} else {
				// 如果不能回收，则返回
				if ( !reference.isItRecyclable() ) {
					return;
				}
			}

		} catch (Exception e) {
			LOGGER.error("recycle err", e);
		}
		
		usedCount.decrementAndGet();
		
		buf.clear();
		queueOffer( buf );
		_shared++;
	}
	
	/**
	 * buffer 引用检测
	 */
	public void referenceCheck() {
		
		for (Entry<Long, ByteBufferReference> entry : bufferReferencMap.entrySet()) {
			ByteBufferReference bufferReference = entry.getValue();
			if ( bufferReference.isTimeout() ) {
				
				ByteBuffer buf = bufferReference.getByteBuffer();
				buf.clear();
				queueOffer( buf );
				_shared++;
				
				usedCount.decrementAndGet();
				
				//
				bufferReference.reset();
				LOGGER.info("buffer re. buffer: {}", bufferReference);
			}
		}
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
		return this.count.get();
	}
	
	public int getUsedCount() {
		return this.usedCount.get();
	}
	
	public long getShared() {
		return _shared;
	}
	
	private void queueOffer(ByteBuffer buffer) {
		this.buffers.offer( buffer );
	}

	private ByteBuffer queuePoll() {
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