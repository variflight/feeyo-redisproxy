package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.util.TimeUtil;

/**
 * This class is thread safe.
 *  
 * @author zhuam
 *
 */
public class ByteBufferBucket implements Comparable<ByteBufferBucket> {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ByteBufferBucket.class );
	private final static int STATE_IDLE = 0;
	private final static int STATE_BORROW = 1;

	private ByteBufferBucketPool bufferPool;
	
	// TODO: ConcurrentLinkedQueue
	// https://bugs.openjdk.java.net/browse/JDK-8137185
	private final ConcurrentLinkedDeque<ByteBuffer> buffers = new ConcurrentLinkedDeque<ByteBuffer>();
	// buffer states, key:buffer address, value: 0:idle 1：borrow
	private final ConcurrentHashMap<Long, AtomicInteger> bufferStates = new ConcurrentHashMap<Long, AtomicInteger>();
	private final ConcurrentHashMap<Long, AbnormalBuffer> abnormalBuffers = new ConcurrentHashMap<Long, AbnormalBuffer>();
	
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
			ByteBuffer buffer = ByteBuffer.allocateDirect(chunkSize);
			initBufferStates( buffer, STATE_IDLE );
			queueOffer( buffer );
			pool.getUsedBufferSize().addAndGet( chunkSize );
		}
	}
	
	//
	public ByteBuffer allocate() {
		
		ByteBuffer bb = queuePoll();
		if (bb != null) {
			long address = ((sun.nio.ch.DirectBuffer) bb).address();
			AtomicInteger byteBufferState = bufferStates.get( address );
			if (byteBufferState == null) {
				byteBufferState = initBufferStates(bb, STATE_IDLE);
			}
			if (byteBufferState.getAndIncrement() == STATE_IDLE) {
				// Clear sets limit == capacity. Position == 0.
				bb.clear();
				return bb;
			} else {
				AbnormalBuffer abnormalBuffer = abnormalBuffers.get(address);
				if (abnormalBuffer == null) {
					abnormalBuffer = new AbnormalBuffer(bb);
					abnormalBuffers.put(address, abnormalBuffer);
				} else {
					abnormalBuffer.setLastUseTime(TimeUtil.currentTimeMillis());
				}
				LOGGER.warn("Direct ByteBuffer allocate warning.... allocate buffer that is been used。buffer: {}, address: {}, count: {}", abnormalBuffer, address, byteBufferState.get());
				return null;
			}
		}
		
		// 桶内内存块不足，创建新的块
		synchronized ( _lock ) {
			// 容量阀值
			long used = bufferPool.getUsedBufferSize().get();
			
			if ( ( used + chunkSize ) < bufferPool.getMaxBufferSize()) { 
				bb = ByteBuffer.allocateDirect( chunkSize );
				initBufferStates(bb, STATE_BORROW);
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
		
		long address = ((sun.nio.ch.DirectBuffer) buf).address();
		AtomicInteger byteBufferState = bufferStates.get( address );
		AbnormalBuffer abnormalBuffer = abnormalBuffers.get( address );
		if (byteBufferState.getAndDecrement() == STATE_BORROW && abnormalBuffer == null) {
			buf.clear();
			queueOffer( buf );
			_shared++;
		} else {
			if (abnormalBuffer == null) {
				abnormalBuffer = new AbnormalBuffer(buf);
				abnormalBuffers.put(address, abnormalBuffer);
			} else {
				abnormalBuffer.setLastUseTime(TimeUtil.currentTimeMillis());
			}
			LOGGER.warn("Direct ByteBuffer recycle warning.... recycle buffer that is been used。buffer: {},address: {}, count: {}", abnormalBuffer, address, byteBufferState.get());
		}
	}

	/**
	 * 异常buffer检测
	 */
	public void abnormalBufferCheck() {
		for (Entry<Long, AbnormalBuffer> entry : abnormalBuffers.entrySet()) {
			AbnormalBuffer abnormalBuffer = entry.getValue();
			if (TimeUtil.currentTimeMillis() - abnormalBuffer.getLastUseTime() > 30 * 60 * 1000) {
				ByteBuffer buffer = abnormalBuffer.getBuffer();
				if (buffer != null) {
					buffer.clear();
					queueOffer( buffer );
					initBufferStates(buffer, STATE_IDLE);
					_shared++;
				}
			}
			abnormalBuffers.remove(entry.getKey());
			LOGGER.info("abnomal byte buffer return. buffer: {}", abnormalBuffer);
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
	
	private AtomicInteger initBufferStates(ByteBuffer buffer, int state) {
		AtomicInteger bufferState = new AtomicInteger(state);
		this.bufferStates.put( ((sun.nio.ch.DirectBuffer) buffer).address(), bufferState);
		return bufferState;
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

	protected class AbnormalBuffer {
		private ByteBuffer buffer;
		private long lastUseTime;
		private long createTime;
		public AbnormalBuffer(ByteBuffer buffer) {
			this.buffer = buffer;
			this.createTime = TimeUtil.currentTimeMillis();
			this.lastUseTime = TimeUtil.currentTimeMillis();
		}
		public ByteBuffer getBuffer() {
			return buffer;
		}
		public long getLastUseTime() {
			return lastUseTime;
		}
		public void setLastUseTime(long lastUseTime) {
			this.lastUseTime = lastUseTime;
		}
		public long getCreateTime() {
			return createTime;
		}
		public void setCreateTime(long createTime) {
			this.createTime = createTime;
		}
		
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("buffer:").append(buffer.toString()).append(". create:").append(createTime).append(". last use:")
					.append(lastUseTime).append(".");
			return sb.toString();
		}
	}
}