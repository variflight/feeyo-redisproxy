package com.feeyo.redis.nio;

import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.redis.nio.util.TimeUtil;


/**
 * 流量监控
 *
 */
public class NetFlowMonitor {
	
	private volatile boolean overproof = false;
	
	private final long maxByteSize;
	
	private AtomicLong[] arrs;
	private int currentIndex;
	
	public NetFlowMonitor(long maxByteSize) {
		
		this.maxByteSize = maxByteSize;

		if (this.maxByteSize > 0) {
			
			arrs = new AtomicLong[ 60 ];
			for (int i = 0; i < arrs.length; i++) {
				arrs[i] = new AtomicLong( maxByteSize );
			}
		}

	}
	
	public boolean pool(long length) {
		
		if ( this.maxByteSize > 0 ) {
			
			long currentTimeMillis = TimeUtil.currentTimeMillis();
			//long currentTimeMillis = System.currentTimeMillis();
			
			int tempIndex = (int) ((currentTimeMillis / 1000) % 60);
			if (currentIndex != tempIndex) {
				synchronized (this) {
					// 这一秒的第一条统计，把对应的存储位的数据置是 max
					if (currentIndex != tempIndex) {

						// reset
						arrs[tempIndex].set( maxByteSize );
						currentIndex = tempIndex;
					}
				}
			}
			
			overproof = decrement(arrs[currentIndex] , length ) > 0;
			return overproof;
		}
		
		return true;
	}
	
	public boolean isOverproof() {
		return overproof;
	}

	
    private final long decrement(AtomicLong atomicLong, long delta) {
        for (;;) {
            long current = atomicLong.get();
            long next = current - delta;
            if (atomicLong.compareAndSet(current, next))
                return next;
        }
    }
    
}
