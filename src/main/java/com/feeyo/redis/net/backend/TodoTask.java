package com.feeyo.redis.net.backend;

public interface TodoTask {

	public void execute(RedisBackendConnection backendCon) throws Exception;
}
