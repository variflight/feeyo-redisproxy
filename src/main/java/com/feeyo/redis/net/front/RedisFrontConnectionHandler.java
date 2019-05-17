package com.feeyo.redis.net.front;

import java.io.IOException;

import com.feeyo.net.nio.NIOHandler;

/** 
 * 负责处理前端发来的 Redis Command
 * 
 * @see http://redis.io/topics/protocol
 * @see http://redisbook.com/preview/server/execute_command.html
 * @see https://wizardforcel.gitbooks.io/redis-doc/content/doc/5.html
 * 
 * @author zhuam
 */

public class RedisFrontConnectionHandler implements NIOHandler<RedisFrontConnection> {
	
	@Override
	public void onConnected(RedisFrontConnection conn) throws IOException {
		// ignore
	}

	@Override
	public void onConnectFailed(RedisFrontConnection conn, Exception e) {
		// ignore
	}

	@Override
	public void onClosed(RedisFrontConnection conn, String reason) {
		
		if ( conn.getSession() != null)
			conn.getSession().frontConnectionClose(reason);
	}

	@Override
	public void handleReadEvent(RedisFrontConnection conn, byte[] data) throws IOException {
		// 分发
		conn.getSession().handle(data);	
	}

}