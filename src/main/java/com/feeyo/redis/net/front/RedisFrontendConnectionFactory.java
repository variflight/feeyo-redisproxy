package com.feeyo.redis.net.front;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.redis.nio.Connection;
import com.feeyo.redis.nio.ConnectionFactory;
import com.feeyo.redis.nio.NetSystem;

/**
 * 
 * @author zhuam
 *
 */
public class RedisFrontendConnectionFactory extends ConnectionFactory {

	@Override
	public Connection make(SocketChannel channel) throws IOException {
		RedisFrontConnection c = new RedisFrontConnection(channel);
		NetSystem.getInstance().setSocketParams(c, true);	// 设置连接的参数
		c.setHandler( new RedisFrontConnectionHandler() );	// 设置NIOHandler
		
		return c;
	}

}
