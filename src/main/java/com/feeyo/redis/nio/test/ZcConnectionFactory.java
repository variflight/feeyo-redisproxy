package com.feeyo.redis.nio.test;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.redis.nio.ClosableConnection;
import com.feeyo.redis.nio.ConnectionFactory;
import com.feeyo.redis.nio.NetSystem;

public class ZcConnectionFactory extends ConnectionFactory {

	@Override
	public ClosableConnection make(SocketChannel channel) throws IOException {
		ZcConnection c = new ZcConnection(channel);
		NetSystem.getInstance().setSocketParams(c, true);	// 设置连接的参数
		c.setHandler( new ZcConnectionHandler() );	// 设置NIOHandler
		return c;
	}

}
