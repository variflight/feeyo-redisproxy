package com.feeyo.redis.net.backend.callback;


import java.io.IOException;

import com.feeyo.redis.net.backend.BackendConnection;

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

	@Override
	public void connectionAcquired(BackendConnection conn) {
		target.connectionAcquired(conn);
	}

	@Override
	public void connectionError(Exception e, BackendConnection conn) {
		target.connectionError(e, conn);
	}

	@Override
	public void handleResponse(BackendConnection conn, byte[] byteBuff) throws IOException {
		target.handleResponse(conn, byteBuff);
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		target.connectionClose(conn, reason);
	}

}
