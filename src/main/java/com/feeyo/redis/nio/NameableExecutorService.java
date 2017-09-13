package com.feeyo.redis.nio;

import java.util.concurrent.ExecutorService;

/**
 * @author wuzh
 */
public interface NameableExecutorService extends ExecutorService {

	public String getName();
}