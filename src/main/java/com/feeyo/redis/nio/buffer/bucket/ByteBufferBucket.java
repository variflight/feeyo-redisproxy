package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
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
	
	// TODO: ConcurrentLinkedQueue
	// https://bugs.openjdk.java.net/browse/JDK-8137185
	private final ConcurrentLinkedDeque<ByteBuffer> buffers = new ConcurrentLinkedDeque<ByteBuffer>();
	
	private final ConcurrentHashMap<Long, ByteBufferReference> bufferReferencMap;
	
	// used count
	private final AtomicInteger used = new AtomicInteger(0);
	
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
		
		this.bufferReferencMap = new ConcurrentHashMap<Long, ByteBufferReference>( count );
		
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
				ByteBufferReference bufferReferenc = bufferReferencMap.get( address );
				if (bufferReferenc == null) {
					bufferReferenc = new ByteBufferReference( address, bb );
					bufferReferencMap.put(address, bufferReferenc);
				}
				
				// 检测
				if ( bufferReferenc.isBorrow( address ) ) {
					
					used.incrementAndGet();
					
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
				
				try {
					long address = ((sun.nio.ch.DirectBuffer) bb).address();
					ByteBufferReference byteBufferState = new ByteBufferReference( address, bb );
					bufferReferencMap.put(address, byteBufferState);
					byteBufferState.isBorrow(address);
				} catch (Exception e) {
					LOGGER.error("allocate err", e);
				}
				
				used.incrementAndGet();
				
				bufferPool.getUsedBufferSize().addAndGet( chunkSize );
			} 
			count++;
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
			ByteBufferReference byteBufferState = bufferReferencMap.get(address);
			if (!byteBufferState.isIdle(address)) {
				return;
			}
		} catch (Exception e) {
			LOGGER.error("recycle err", e);
		}
		
		used.decrementAndGet();
		
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
				
				recycle( bufferReference.getByteBuffer() );
				
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
	
	public int getBufferUsedCount() {
		return this.used.get();
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