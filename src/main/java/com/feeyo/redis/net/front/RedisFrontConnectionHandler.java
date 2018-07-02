package com.feeyo.redis.net.front;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.NIOHandler;
import com.feeyo.net.nio.util.StringUtil;

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
	
	private static Logger LOGGER = LoggerFactory.getLogger( RedisFrontConnectionHandler.class );
	
	@Override
	public void onConnected(RedisFrontConnection conn) throws IOException {
		if ( LOGGER.isDebugEnabled() )
			LOGGER.debug("onConnected(): {}", conn);
	}

	@Override
	public void onConnectFailed(RedisFrontConnection conn, Exception e) {
		if ( LOGGER.isDebugEnabled() )	
			LOGGER.debug("onConnectFailed(): {}", conn);
	}

	@Override
	public void onClosed(RedisFrontConnection conn, String reason) {
		if ( LOGGER.isDebugEnabled() )
			LOGGER.debug("onClosed(): {}, {}", conn, reason);
		
		if ( conn.getSession() != null)
			conn.getSession().frontConnectionClose(reason);
	}

	@Override
	public void handleReadEvent(RedisFrontConnection conn, byte[] data) throws IOException {
		// 日志HEX
		if ( LOGGER.isDebugEnabled() ) {
			final String hexs = StringUtil.dumpAsHex(data, 0, data.length);
			LOGGER.debug("C#{} front request len = {}, buffer bytes\n {}", 
					new Object[]{ conn.getId(), data.length, hexs });
		}

		// 分发
		conn.getSession().handle(data);	
	}

}