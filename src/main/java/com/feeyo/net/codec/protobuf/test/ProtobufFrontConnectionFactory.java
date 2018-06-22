package com.feeyo.net.codec.protobuf.test;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.ClosableConnection;
import com.feeyo.net.nio.ConnectionFactory;
import com.feeyo.net.nio.NetSystem;

public class ProtobufFrontConnectionFactory extends ConnectionFactory {

	@Override
	public ClosableConnection make(SocketChannel channel) throws IOException {
		ProtobufFrontConnection c = new ProtobufFrontConnection(channel);
		NetSystem.getInstance().setSocketParams(c, true);	// 设置连接的参数
		c.setHandler( new ProtobufFrontConnectionHandler() );	// 设置NIOHandler
		return c;
	}

}
