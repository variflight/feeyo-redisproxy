package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.buffer.BufferPool;
import com.feeyo.redis.nio.util.TimeUtil;

/**
 * 堆外内存池
 * 
 * @author zhuam
 *
 */
public class ByteBufferBucketPool extends BufferPool {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ByteBufferBucketPool.class );
	
	private TreeMap<Integer, ByteBufferBucket> _buckets;
	
	private long sharedOptsCount;
	
	private ScheduledExecutorService bufferCheckExecutor = null;
	private final AtomicBoolean checking = new AtomicBoolean( false );
	private final ConcurrentHashMap<Long, ByteBufferState> byteBufferStates;
	
	public ByteBufferBucketPool(long minBufferSize, long maxBufferSize, int minChunkSize, int increment, int maxChunkSize) {
		
		super(minBufferSize, maxBufferSize, minChunkSize, increment, maxChunkSize);
		
		int bucketsCount = maxChunkSize / increment;
		this._buckets = new TreeMap<Integer, ByteBufferBucket>();
		this.byteBufferStates = new ConcurrentHashMap<Long, ByteBufferState>();
		
		// 平均分配初始化的桶size 
		long bucketBufferSize = minBufferSize / bucketsCount;
		
		// 初始化桶 
		int chunkSize = 0;
		for (int i = 0; i < bucketsCount; i++) {
			chunkSize += increment;
			int chunkCount = (int) (bucketBufferSize / chunkSize);
			ByteBufferBucket bucket = new ByteBufferBucket(this, chunkSize, chunkCount);
			this._buckets.put(bucket.getChunkSize(), bucket);
		}
		
		// 自动扩容
		Runnable runable = new Runnable() {

			@Override
			public void run() {

				if (!checking.compareAndSet(false, true)) {
					return;
				}

				try {
					for (Entry<Long, ByteBufferState> entry : byteBufferStates.entrySet()) {
						ByteBufferState byteBufferState = entry.getValue();
						if (byteBufferState.isUnHealthyTimeOut()) {
							byteBufferState.setHealthy(true);
							byteBufferState.getState().set(ByteBufferState.STATE_BORROW);
							recycle(byteBufferState.getByteBuffer());
							LOGGER.info("abnomal byte buffer return. buffer: {}", byteBufferState);
						}
					}
				} catch (Exception e) {
					LOGGER.warn("ByteBufferBucket abnormalBufferCheck err:", e);

				} finally {
					checking.set(false);
				}
			}

		};

		// 异常监控
		bufferCheckExecutor = Executors.newSingleThreadScheduledExecutor();
		bufferCheckExecutor.scheduleAtFixedRate(runable, 120L, 300L, TimeUnit.SECONDS);
		
	}
	
	//根据size寻找 桶
	private ByteBufferBucket bucketFor(int size) {
		if (size <= minChunkSize)
			return null;
		
		Map.Entry<Integer, ByteBufferBucket> entry = this._buckets.ceilingEntry( size );
		return entry == null ? null : entry.getValue();

	}
	
	//TODO : debug err, TMD, add temp synchronized
	
	@Override
	public ByteBuffer allocate(int size) {		
	    	
		ByteBuffer byteBuf = null;
		
		// 根据容量大小size定位到对应的桶Bucket
		ByteBufferBucket bucket = bucketFor(size);
		if ( bucket != null) {
			byteBuf = bucket.allocate();
		}
		
		// 堆内
		if (byteBuf == null) {
			byteBuf = ByteBuffer.allocate( size );
			
		// 二次校验
		} else {
			long address = ((sun.nio.ch.DirectBuffer) byteBuf).address();
			ByteBufferState byteBufferState = byteBufferStates.get(address);
			if (byteBufferState == null) {
				byteBufferState = new ByteBufferState(byteBuf);
				byteBufferStates.put(address, byteBufferState);
			}
			
			if (byteBufferState.getState().getAndIncrement() != ByteBufferState.STATE_IDLE || !byteBufferState.isHealthy()) {
				byteBufferState.setHealthy(false);
				LOGGER.warn(
						"Direct ByteBuffer allocate warning.... allocate buffer that is been used。ByteBufferState: {}, address: {}",
						byteBufferState, address);
				return ByteBuffer.allocate( size );
			}
			
			byteBufferState.setLastUseTime(TimeUtil.currentTimeMillis());
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
		
		long address = ((sun.nio.ch.DirectBuffer) buf).address();
		ByteBufferState byteBufferState = byteBufferStates.get(address);
		if (byteBufferState.isHealthy()) {
			if (byteBufferState.getState().getAndDecrement() != ByteBufferState.STATE_BORROW) {
				byteBufferState.setHealthy(false);
				LOGGER.warn(
						"Direct ByteBuffer recycle warning.... recycle buffer that is been used。ByteBufferState: {},address: {}",
						byteBufferState, address);
				return;
			}
		} else {
			return;
		}
      	
		ByteBufferBucket bucket = bucketFor( buf.capacity() );
		if (bucket != null) {
			bucket.recycle( buf );
			sharedOptsCount++;

		} else {
			LOGGER.warn("Trying to put a buffer, not created by this pool! Will be just ignored");
		}
	}

	public synchronized ByteBufferBucket[] buckets() {
		
		ByteBufferBucket[] tmp = new ByteBufferBucket[ _buckets.size() ];
		int i = 0;
		for(ByteBufferBucket b: _buckets.values()) {
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
