package com.feeyo.redis.net.front.handler;

import java.io.IOException;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.DirectTransTofrontCallBack;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.nio.util.TimeUtil;

/**
 * 支持 BLPOP BRPOP 阻塞特性
 */
public class BlockCommandHandler extends AbstractCommandHandler {
	
	private RedisBackendConnection usedConnection;
	
	public BlockCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}

	@Override
	protected void commonHandle(RouteResult rrs) throws IOException {
		RouteResultNode node = rrs.getRouteResultNodes().get(0);
		RedisRequest request = rrs.getRequests().get(0);
		
		String cmd = new String(request.getArgs()[0]).toUpperCase();
		byte[] requestKey = request.getNumArgs() > 1 ? request.getArgs()[1] : null;
		
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd( cmd );
		frontCon.getSession().setRequestKey(requestKey);
		frontCon.getSession().setRequestSize(request.getSize());
		
		// 透传
		usedConnection = writeToBackend(node.getPhysicalNode(), request.encode(), new BlockDirectTransTofrontCallBack());
	}
	
	private class BlockDirectTransTofrontCallBack extends DirectTransTofrontCallBack {
		
		@Override
		public void connectionError(Exception e, RedisBackendConnection backendCon) {
			usedConnection = null;
		}
		
		@Override
		public void connectionClose(RedisBackendConnection backendCon, String reason) {
			usedConnection = null;
		}
		
		@Override
		public void handleResponse(RedisBackendConnection backendCon, byte[] byteBuff) throws IOException {
			// handler释放后端链接
			usedConnection = null;
			
			try {
				super.handleResponse(backendCon, byteBuff);
			} finally {
				frontCon.releaseLock();
			}
		}
	}

	@Override
	public void frontConnectionClose(String reason) {
		super.frontConnectionClose(reason);
		
		if (usedConnection != null) {
			usedConnection.close(reason);
			usedConnection = null;
		}
	}
	
	@Override
	public void frontHandlerError(Exception e) {
		super.frontHandlerError(e);
		
		if (usedConnection != null) {
			usedConnection.close( e.getMessage() );
			usedConnection = null;
		}
	}
}
