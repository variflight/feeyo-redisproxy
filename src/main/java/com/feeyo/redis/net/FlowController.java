package com.feeyo.redis.net;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.redis.nio.util.TimeUtil;

public class FlowController {
	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	private AtomicLong[] net;
	private volatile int index;
	private volatile boolean isOutOfFlow = false;
	private boolean isOpen;
	private final long size;
	
	public FlowController(long size) {
		this.size = size;
		this.isOpen = this.size >= 0;
		// 开启流量控制
		if (isOpen) {
			this.net = new AtomicLong[60];
			for (int i = 0; i < this.net.length; i++)
				this.net[i] = new AtomicLong(size);
			
			this.init();
		}
	}
		
	private void init() {
		executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				// 更新当前坐标
				index = getIndex();

				// 判定是否需要限流
				if (index == 0) {
					isOutOfFlow = net[59].get() < 0;
				} else {
					isOutOfFlow = net[index - 1].get() < 0;
				}
				
				// 更新其他容量
				for (int i = 0; i < net.length; i++) {
					if (i != index) {
						net[i].set(size);
					}
				}
			}
		}, 0, 1, TimeUnit.SECONDS);
	}
	
	public boolean pool(long size) {
		if (isOpen) {
			return decrement(this.net[index], size) > 0;
		}
		return true;
	}
	
	public boolean isOutOfFlow() {
		return isOutOfFlow;
	}
	
	private int getIndex() {
		long currentTimeMillis = TimeUtil.currentTimeMillis();
        return (int) ((currentTimeMillis / 1000) % 60);
	}
	
	/**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    public final long decrement(AtomicLong al, long delta) {
        for (;;) {
            long current = al.get();
            long next = current - delta;
            if (al.compareAndSet(current, next))
                return next;
        }
    }
}
