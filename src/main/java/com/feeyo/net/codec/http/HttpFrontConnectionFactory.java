package com.feeyo.net.codec.http;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.ClosableConnection;
import com.feeyo.net.nio.ConnectionFactory;
import com.feeyo.net.nio.NetSystem;

public class HttpFrontConnectionFactory extends ConnectionFactory {

	@Override
	public ClosableConnection make(SocketChannel channel) throws IOException {
		HttpFrontConnection c = new HttpFrontConnection(channel);
		NetSystem.getInstance().setSocketParams(c, true);	// 设置连接的参数
		c.setHandler( new HttpFrontConnectionHandler() );	// 设置NIOHandler
		return c;
	}

}
