package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.buffer.BufferPool;

/**
 * 堆外内存池
 * 
 * @author zhuam
 *
 */
public class ByteBufferBucketPool extends BufferPool {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ByteBufferBucketPool.class );
	
	private TreeMap<Integer, AbstractByteBufferBucket> _buckets;
	
	private long sharedOptsCount;
	
	public ByteBufferBucketPool(long minBufferSize, long maxBufferSize, int decomposeBufferSize,
			int minChunkSize, int[] increments, int maxChunkSize, int threadLocalPercent) {
		
		super(minBufferSize, maxBufferSize, decomposeBufferSize, minChunkSize, increments, maxChunkSize);
		
//		int bucketsCount = maxChunkSize / increments;
		
		int bucketsCount;
		if (increments.length > 1) {
			bucketsCount = increments.length;
		} else {
			bucketsCount = maxChunkSize / increments[0];
		}
		
		this._buckets = new TreeMap<Integer, AbstractByteBufferBucket>();
		
		// 平均分配初始化的桶size 
		long bucketBufferSize = minBufferSize / bucketsCount;
		
		// 初始化桶 
		int chunkSize = 0;
		for (int i = 0; i < bucketsCount; i++) {
			chunkSize += increments[i >= increments.length ? 0 : i];
			int chunkCount = (int) (bucketBufferSize / chunkSize);
			boolean isExpand =  chunkSize <= 262144 ? true: false; 	// 256K内的块 支持自动扩容
			
			// 测试结果 队列长度2048的时候效果就没那么显著了。
			AbstractByteBufferBucket bucket;
			if (chunkCount > 2000) {
				bucket = new SegmentByteBufferBucket(this, chunkSize, chunkCount, isExpand, threadLocalPercent);
			} else {
				bucket = new CommonByteBufferBucket(this, chunkSize, chunkCount, isExpand, threadLocalPercent);
			}
			
			this._buckets.put(bucket.getChunkSize(), bucket);
		}
		
		// 引用检测
		ByteBufferReferenceUtil.referenceCheck(_buckets);
	}
	
	//根据size寻找 桶
	private AbstractByteBufferBucket bucketFor(int size) {
		if (size <= minChunkSize)
			return null;
		
		Map.Entry<Integer, AbstractByteBufferBucket> entry = this._buckets.ceilingEntry( size );
		return entry == null ? null : entry.getValue();

	}
	
	//TODO : debug err, TMD, add temp synchronized
	
	@Override
	public ByteBuffer allocate(int size) {		
	    	
		ByteBuffer byteBuf = null;
		
		// 根据容量大小size定位到对应的桶Bucket
		AbstractByteBufferBucket bucket = bucketFor(size);
		if ( bucket != null) {
			byteBuf = bucket.allocate();
		}
		
		// 堆内
		if (byteBuf == null) {
			byteBuf =  ByteBuffer.allocate( size );
		}
		return byteBuf;

	}

	@Override
	public void recycle(ByteBuffer buf) {
		if (buf == null) {
			return;
		}
		
		if( !buf.isDirect() ) {
			return;
		}
      	
		AbstractByteBufferBucket bucket = bucketFor( buf.capacity() );
		if (bucket != null) {
			bucket.recycle( buf );
			sharedOptsCount++;

		} else {
			LOGGER.warn("Trying to put a buffer, not created by this pool! Will be just ignored");
		}
	}

	public synchronized AbstractByteBufferBucket[] buckets() {
		
		AbstractByteBufferBucket[] tmp = new AbstractByteBufferBucket[ _buckets.size() ];
		int i = 0;
		for(AbstractByteBufferBucket b: _buckets.values()) {
			tmp[i] = b;
			i++;
		}
		return tmp;
	}
	
	@Override
	public long getSharedOptsCount() {
		return sharedOptsCount;
	}

	@Override
	public long capacity() {
		return this.maxBufferSize;
	}

	@Override
	public long size() {
		return this.usedBufferSize.get();
	}

	@Override
	public int getChunkSize() {
		return this.getMinChunkSize();
	}

	@Override
	public ConcurrentHashMap<Long, Long> getNetDirectMemoryUsage() {
		return null;
	}
}
