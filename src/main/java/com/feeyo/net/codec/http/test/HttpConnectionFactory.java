package com.feeyo.net.codec.http.test;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.ClosableConnection;
import com.feeyo.net.nio.ConnectionFactory;
import com.feeyo.net.nio.NetSystem;

public class HttpConnectionFactory extends ConnectionFactory {

	@Override
	public ClosableConnection make(SocketChannel channel) throws IOException {
		HttpConnection c = new HttpConnection(channel);
		NetSystem.getInstance().setSocketParams(c, true);	// 设置连接的参数
		
		//
		HttpConnectionHandler connectionHandler = new HttpConnectionHandler();
		connectionHandler.registerHandler(HttpConnectionHandler.HTTP_METHOD_POST, 
				"/proto/eraftpb/message", new ProtobufRequestHandler());
		
		c.setHandler( connectionHandler );	// 设置NIOHandler
		return c;
	}

}
