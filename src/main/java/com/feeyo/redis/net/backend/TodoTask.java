package com.feeyo.redis.net.backend;

public interface TodoTask {

	public void execute(BackendConnection conn) throws Exception;
}
