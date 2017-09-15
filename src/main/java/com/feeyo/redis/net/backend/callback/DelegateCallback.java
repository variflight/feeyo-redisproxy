package com.feeyo.redis.net.backend.callback;


import java.io.IOException;

import com.feeyo.redis.net.backend.RedisBackendConnection;

/**
 * 代理
 * 
 * @author zhuam
 *
 */
public abstract class DelegateCallback implements BackendCallback {
	
	protected BackendCallback target;
	
	public DelegateCallback(BackendCallback target) {
		this.target = target;
	}

//	@Override
//	public void connectionAcquired(RedisBackendConnection conn) {
//		target.connectionAcquired(conn);
//	}

	@Override
	public void connectionError(Exception e, RedisBackendConnection conn) {
		target.connectionError(e, conn);
	}

	@Override
	public void handleResponse(RedisBackendConnection conn, byte[] byteBuff) throws IOException {
		target.handleResponse(conn, byteBuff);
	}

	@Override
	public void connectionClose(RedisBackendConnection conn, String reason) {
		target.connectionClose(conn, reason);
	}

}
