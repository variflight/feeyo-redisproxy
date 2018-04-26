package com.feeyo.redis.config;

import java.util.concurrent.atomic.AtomicInteger;

public class UserFlowLimitCfg {
	
	private AtomicInteger index = new AtomicInteger(0);
	private int throughPercentage;
	
	public UserFlowLimitCfg(int throughPercentage) {
		this.throughPercentage = throughPercentage;
	}
	
	public boolean isOk() {
		return getIndex() < throughPercentage;
	}
	
	private int getIndex() {
		for (;;) {
			int current = index.get();
			int next = current >= 99 ? 0 : current + 1;
			if (index.compareAndSet(current, next))
				return current;
		}
	}

}
