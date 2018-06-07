package com.feeyo.redis.nio.test;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.redis.nio.AbstractConnection;
import com.feeyo.redis.nio.ConnectionFactory;
import com.feeyo.redis.nio.NetSystem;

public class ZeroCopyConnectionFactory extends ConnectionFactory {

	@Override
	public AbstractConnection make(SocketChannel channel) throws IOException {
		ZeroCopyConnection c = new ZeroCopyConnection(channel);
		NetSystem.getInstance().setSocketParams(c, true);	// 设置连接的参数
		c.setHandler( new ZeroCopyConnectionHandler() );	// 设置NIOHandler
		return c;
	}

}
