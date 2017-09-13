package com.feeyo.util;

import java.util.concurrent.LinkedTransferQueue;

import com.feeyo.redis.nio.NameableExecutor;
import com.feeyo.redis.nio.NameableThreadFactory;

/**
 * 生成一个有名字的（Nameable）Executor，容易进行跟踪和监控
 * 
 * @author wuzh
 */
public class ExecutorUtil {

	public static final NameableExecutor create(String name, int size) {
        return create(name, size, true);
    }

    private static final NameableExecutor create(String name, int size, boolean isDaemon) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, new LinkedTransferQueue<Runnable>(), factory);
    }
}