package com.feeyo.redis.nio;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

public class NamebleScheduledExecutor extends ScheduledThreadPoolExecutor {
	
	private final String name;

	public NamebleScheduledExecutor(String name, int corePoolSize, ThreadFactory threadFactory) {
		super(corePoolSize, threadFactory);
		this.name = name;
	}

	public String getName() {
		return name;
	}

}
