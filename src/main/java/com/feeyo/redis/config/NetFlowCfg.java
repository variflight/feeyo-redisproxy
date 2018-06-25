package com.feeyo.redis.config;

import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.net.nio.util.TimeUtil;

public class NetFlowCfg {

	private final String password;
	private final int perSecondMaxSize;
	private final int requestMaxSize;
	private boolean isControl;
	
	//
	private int currentIndex;
	private AtomicLong[] sencondSizes;

	public NetFlowCfg(String password, int perSecondMaxSize, int requestMaxSize, boolean isControl) {
		this.password = password;
		this.perSecondMaxSize = perSecondMaxSize;
		this.requestMaxSize = requestMaxSize;
		this.isControl = isControl;

		if (this.perSecondMaxSize > 0) {

			sencondSizes = new AtomicLong[60];
			for (int i = 0; i < sencondSizes.length; i++) {
				sencondSizes[i] = new AtomicLong( perSecondMaxSize );
			}
		}
	}

	/**
	 * @param length
	 * @return 是否超出流量
	 */
	public boolean pool(long length) {
		
		if ( length > requestMaxSize) {
			return true;
		}

		if (this.perSecondMaxSize > 0 && length > 0) {

			long currentTimeMillis = TimeUtil.currentTimeMillis();
			// long currentTimeMillis = System.currentTimeMillis();

			int tempIndex = (int) ((currentTimeMillis / 1000) % 60);
			if (currentIndex != tempIndex) {
				synchronized (this) {
					// 这一秒的第一条统计，把对应的存储位的数据置是 max
					if (currentIndex != tempIndex) {
						
						// reset
						sencondSizes[tempIndex].set(perSecondMaxSize);
						currentIndex = tempIndex;
					}
				}
			}

			return decrement(sencondSizes[currentIndex], length) <= 0;
		}

		return true;
	}

	public String getPassword() {
		return password;
	}

	public int getPerSecondMaxSize() {
		return perSecondMaxSize;
	}

	public int getRequestMaxSize() {
		return requestMaxSize;
	}
	
	public boolean isControl() {
		return isControl;
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
