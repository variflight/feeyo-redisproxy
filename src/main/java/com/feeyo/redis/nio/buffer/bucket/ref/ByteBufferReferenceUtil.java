package com.feeyo.redis.nio.buffer.bucket.ref;

import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.buffer.bucket.AbstractBucket;

public class ByteBufferReferenceUtil {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ByteBufferReferenceUtil.class );
	
	// 
	private static ScheduledExecutorService referenceExecutor = Executors.newSingleThreadScheduledExecutor();

	private ByteBufferReferenceUtil() {}
	
	public static void referenceCheck(TreeMap<Integer, AbstractBucket> buckets) {
		
		// 5 分钟
		referenceExecutor.scheduleAtFixedRate(new ReleaseTask(buckets), 120L, 300L, TimeUnit.SECONDS);
	}
	
	private static final class ReleaseTask implements Runnable {
		
		private final TreeMap<Integer, AbstractBucket> buckets;
		private final AtomicBoolean checking = new AtomicBoolean( false );
		
		ReleaseTask(TreeMap<Integer, AbstractBucket> buckets) {
			this.buckets = buckets;
		}

		@Override
		public void run() {
			
			if (!checking.compareAndSet(false, true)) {
				return;
			}

			try {
				Iterator<AbstractBucket> it = buckets.values().iterator();
				while( it.hasNext() ) {
					AbstractBucket bucket = it.next();
					bucket.releaseTimeoutBuffer();
				}
			} catch (Exception e) {
				LOGGER.warn("##referenceCheck err:", e);

			} finally {
				checking.set(false);
			}
		}
		
	}
}
