package com.feeyo.redis.nio.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 缓冲池
 */
public abstract class BufferPool {
	
	//
	protected long minBufferSize;
	protected long maxBufferSize;
	protected AtomicLong usedBufferSize = new AtomicLong(0); 
	
	protected int minChunkSize;
	protected int[] increments;
	protected int maxChunkSize;
	protected int decomposeBufferSize;	// 用于大buffer 分解
	public static final String LOCAL_BUF_THREAD_PREX = "$_";
	
	public BufferPool(long minBufferSize, long maxBufferSize, int decomposeBufferSize, 
			int minChunkSize, int[] increments, int maxChunkSize) {
		
		this.minBufferSize = minBufferSize;
		this.maxBufferSize = maxBufferSize;
		this.decomposeBufferSize = decomposeBufferSize;
		
		if (minChunkSize <= 0)
			minChunkSize = 0;
		
		for (int increment : increments) {
			if (increment <= 0)
				increment = 1024;
		}
		
		if (maxChunkSize <= 0) {
			maxChunkSize = 64 * 1024;
		}
		
		if ( decomposeBufferSize <= 0 ) {
			decomposeBufferSize = 64 * 1024;
		}
		
		
		// 均值增加 pool
		if (increments.length == 1) {
			
			int increment = increments[0];
			//最小 size不能大于增量
			if (minChunkSize > increment)
				throw new IllegalArgumentException("minChunkSize >= increment");
			//最大size 必须是增量的整数倍，并且增量不能大于最大的size
			if ((maxChunkSize % increment) != 0 || increment >= maxChunkSize)
				throw new IllegalArgumentException("increment must be a divisor of maxChunkSize");
			
		// 自定义pool，按照配置增加
		} else {
			int minIncrement = increments[0];
			//最小 size不能大于最小的增量
			if (minChunkSize > minIncrement)
				throw new IllegalArgumentException("minChunkSize >= minChunkSize");
			
			int incrementTotal = 0;
			for (int increment : increments)
				incrementTotal = incrementTotal + increment;
			//最大size 必须是所有增量的和
			if ( maxChunkSize != incrementTotal)
				throw new IllegalArgumentException("All the increments should be equal to the maxChunkSize");
			
		}
		
		this.minChunkSize = minChunkSize;
		this.increments = increments;
		this.maxChunkSize = maxChunkSize;
	}
	
	public long getMinBufferSize() {
		return minBufferSize;
	}

	public long getMaxBufferSize() {
		return maxBufferSize;
	}
	
	public int getDecomposeBufferSize() {
		return decomposeBufferSize;
	}


	public AtomicLong getUsedBufferSize() {
		return usedBufferSize;
	}
	
	
	public int getMinChunkSize() {
		if( minChunkSize <= 0 ) {
			return increments[0];
		}
		return minChunkSize;
	}
	
	public int getMaxChunkSize() {
		return maxChunkSize;
	}
	
    public int getChunkSize() {
    	return this.getMinChunkSize();
    }
	
	
    public abstract ByteBuffer allocate(int size);
    public abstract void recycle(ByteBuffer theBuf);
    
    public abstract long capacity();
    public abstract long size();
    public abstract long getSharedOptsCount();
    
    public abstract ConcurrentHashMap<Long,Long> getNetDirectMemoryUsage();
    
}
