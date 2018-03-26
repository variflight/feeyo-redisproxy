package com.feeyo.redis.nio.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 线程安全的共享 ByteBufferPool
 * 
 * @author wuzhih
 * @author zhuam
 *
 */
public class DefaultBufferPool extends BufferPool {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BufferPool.class );
	
	private final Queue<ByteBuffer> buffers = new ConcurrentLinkedQueue<ByteBuffer>();
	
	private final int chunkSize;
	
	private long initCount = 0;
	private long maxCount = 0;
	
	private boolean isDirectOOM = false;
	private long sharedOptsCount;
	
	//
	private AtomicLong count = new AtomicLong(0);
	
	public DefaultBufferPool(long minBufferSize, long maxBufferSize, int decomposeBufferSize,
			int minChunkSize, int[] increments, int maxChunkSize  ) {
		super(minBufferSize, maxBufferSize, decomposeBufferSize, minChunkSize, increments, maxChunkSize);
		
		this.chunkSize = getMinChunkSize();
		
		this.initCount = minBufferSize / chunkSize;
		this.initCount = (minBufferSize % chunkSize == 0) ? initCount : initCount + 1;
		this.maxCount =  maxBufferSize / chunkSize;
		
		// 保护
		if ( this.initCount > this.maxCount ) {
			this.maxCount = this.initCount;
		}
		
		for (long i = 0; i < initCount; i++) {
			this.buffers.offer( createDirectBuffer( chunkSize ) );
			this.count.incrementAndGet();
		}
		
	}


	/**
	 * 申请一个ByteBuffer 内存，用完需要在合适的时间释放，
	 * @see recycle
	 * @return
	 */
	public ByteBuffer allocate(int size) {
		if ( size <= this.chunkSize) {
			return allocate();
		} else {
			//LoggerUtil.warn("allocate buffer size large than default chunksize:" + this.chunkSize + " he want " + size);
			return createTempBuffer(size);
		}
	}
	
	public ByteBuffer allocate() {
		
		ByteBuffer bb = buffers.poll();
		if (bb != null) {
	      return bb;
	    }
		
		// CAS 自旋
		for(;;) {
		      long c = this.count.longValue();
		      if (c >= this.maxCount || isDirectOOM) {
		    	  bb = this.createTempBuffer(chunkSize);
		    	  return bb;
		      }
		      
		      if (!this.count.compareAndSet(c, c + 1)) {
		          continue;
		      }
		      
		      try {
		    	  bb = this.createDirectBuffer(chunkSize);
		      } catch (final OutOfMemoryError oom) {
		    	  LOGGER.warn("Direct buffer OOM occurs: so allocate from heap", oom);
		    	  isDirectOOM = true;
		    	  bb = this.createTempBuffer(chunkSize);
		      }
		      return bb;  
		}
	}

	/**
	 * 回收ByteBuffer
	 */
	public void recycle(ByteBuffer buffer) {		
		if ( !checkValidBuffer( buffer ) ) {
			return;
		}
		
		sharedOptsCount++;
		this.buffers.offer( buffer );
	}

	private boolean checkValidBuffer(ByteBuffer buffer) {
		// 拒绝回收null和容量大于chunkSize的缓存
		if (buffer == null || !buffer.isDirect()) {
			return false;
		} else if (buffer.capacity() != chunkSize) {
			LOGGER.warn("cant' recycle a buffer not equals my pool chunksize " + chunkSize + "  he is " + buffer.capacity());
			return false;
		}
		buffer.clear();
		return true;
	}

	public long getMinBufferSize() {
		return minBufferSize;
	}
	
    public long getMaxBufferSize() {
		return maxBufferSize;
	}

	public int getChunkSize() {
		return chunkSize;
	}
	
	@Override
	public long getSharedOptsCount() {
		return sharedOptsCount;
	}

	public long size() {
		return this.count.get() * this.chunkSize;
	}
	
	public long capacity() {
		return this.maxBufferSize;
	}

	public boolean isDirectOOM() {
		return isDirectOOM;
	}

	private ByteBuffer createTempBuffer(int size) {
		return ByteBuffer.allocate(size);
	}

	private ByteBuffer createDirectBuffer(int size) {
		ByteBuffer buf = ByteBuffer.allocateDirect(size);
		return buf;
	}
	
	public static void main(String[] args) {
		DefaultBufferPool pool = new DefaultBufferPool(1024*1024 * 10, 100*1024*1024, 128, 1024, new int[] {64 * 1024}, 64 * 1024);
		long i = pool.capacity();		
		System.out.println(i);
		
		List<ByteBuffer> all = new ArrayList<ByteBuffer>();
		for (int j = 0; j <= i; j++) {
			all.add(pool.allocate());
		}
		
		for (ByteBuffer buf : all) {
			pool.recycle(buf);
		}
		System.out.println(pool.size());
		
	}

	@Override
	public ConcurrentHashMap<Long, Long> getNetDirectMemoryUsage() {
		return null;
	}
}