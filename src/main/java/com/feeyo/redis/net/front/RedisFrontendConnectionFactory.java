package com.feeyo.redis.net.front;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.nio.AbstractConnection;
import com.feeyo.redis.nio.ConnectionFactory;
import com.feeyo.redis.nio.NetSystem;

/**
 * 
 * @author zhuam
 *
 */
public class RedisFrontendConnectionFactory extends ConnectionFactory {

	@Override
	public AbstractConnection make(SocketChannel channel) throws IOException {
		RedisFrontConnection c = new RedisFrontConnection(channel);
		NetSystem.getInstance().setSocketParams(c, true);	// 设置连接的参数
		c.setHandler( new RedisFrontConnectionHandler() );	// 设置NIOHandler
		c.setFlowMonitor( RedisEngineCtx.INSTANCE().getFlowMonitor() );
		return c;
	}

}
