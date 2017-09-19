package com.feeyo.redis.net.front.handler;

import java.io.IOException;

import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.DirectTransTofrontCallBack;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.nio.NetSystem;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.redis.virtualmemory.Message;
import com.feeyo.redis.virtualmemory.Util;

public class DefaultCommandHandler extends AbstractCommandHandler {
	
	//
	private RedisBackendConnection backendCon;
	
	public DefaultCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}

	@Override
	protected void commonHandle(RouteResult routeResult) throws IOException {
		
		RouteResultNode node = routeResult.getRouteResultNodes().get(0);
		RedisRequest request = routeResult.getRequests().get(0);
		
		String cmd = new String(request.getArgs()[0]).toUpperCase();
		byte[] requestKey = request.getNumArgs() > 1 ? request.getArgs()[1] : null;
		
		int requestSize = request.getSize();
		
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd( cmd );
		frontCon.getSession().setRequestKey(requestKey);
		frontCon.getSession().setRequestSize(request.getSize());
		
		// 透传
		backendCon = writeToBackend(node.getPhysicalNode(), request.encode(), new DirectTransTofrontCallBack());
		
		// 大于256K 临时存放虚拟内存
		if ( requestSize > 1024 * 256 ) {
			Message msg = new Message();
			msg.setBody( request.toString().getBytes() );
			msg.setBodyCRC( Util.crc32(msg.getBody()) );			// body CRC 
			msg.setQueueId( 0 );									// queue id 后期可考虑利用
			msg.setSysFlag( 0 );
			msg.setBornTimestamp( System.currentTimeMillis() );
			backendCon.setSendData( RedisEngineCtx.INSTANCE().getVirtualMemoryService().putMessage( msg ) );
		}

	}

	@Override
	public void frontConnectionClose(String reason) {
		super.frontConnectionClose(reason);
		
		// hold 检测一些
		if ("stream closed".equals(reason) && backendCon != null && !backendCon.isClosed()	&& backendCon.isBorrowed()) {
			NetSystem.getInstance().addTimeoutConnection(backendCon.getId());
		}
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
