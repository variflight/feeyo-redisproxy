package com.feeyo.redis.nio;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author wuzh
 */
public class NameableExecutor extends ThreadPoolExecutor implements NameableExecutorService{
	
    protected final String name;

    public NameableExecutor(String name, int size, BlockingQueue<Runnable> queue, ThreadFactory factory) {
        super(size, size, Long.MAX_VALUE, TimeUnit.NANOSECONDS, queue, factory);
        this.name = name;
    }
    
    public NameableExecutor(String name, int corePoolSize, int maximumPoolSize, int keepalive, 
    		TimeUnit unit, BlockingQueue<Runnable> queue, ThreadFactory factory) {
    	
        super(corePoolSize, maximumPoolSize, keepalive, unit, queue, factory);
        this.name = name;
    }

    public String getName() {
        return name;
    }

}