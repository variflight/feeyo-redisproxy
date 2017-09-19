package com.feeyo.redis.net.front.handler;

import java.io.IOException;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.net.backend.callback.DirectTransTofrontCallBack;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.nio.util.TimeUtil;

public class DefaultCommandHandler extends AbstractCommandHandler {
	
	public DefaultCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}

	@Override
	protected void commonHandle(RouteResult routeResult) throws IOException {
		
		RouteResultNode node = routeResult.getRouteResultNodes().get(0);
		RedisRequest request = routeResult.getRequests().get(0);
		
		String cmd = new String(request.getArgs()[0]).toUpperCase();
		byte[] requestKey = request.getNumArgs() > 1 ? request.getArgs()[1] : null;
		
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd( cmd );
		frontCon.getSession().setRequestKey(requestKey);
		frontCon.getSession().setRequestSize(request.getSize());
		
		// 透传
		writeToBackend(node.getPhysicalNode(), request.encode(), new DirectTransTofrontCallBack());
	}

	@Override
	public void frontConnectionClose(String reason) {
		super.frontConnectionClose(reason);
	}
	
	
	@Override
    public void backendConnectionError(Exception e) {
		
		super.backendConnectionError(e);
		
		if( frontCon != null && !frontCon.isClosed() ) {
			frontCon.writeErrMessage(e.toString());
		}
	}

	@Override
	public void backendConnectionClose(String reason) {
		
		super.backendConnectionClose(reason);

		if( frontCon != null && !frontCon.isClosed() ) {
			frontCon.writeErrMessage( reason );
		}
	}
	
}
