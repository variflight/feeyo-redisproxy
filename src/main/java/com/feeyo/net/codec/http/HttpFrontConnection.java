package com.feeyo.net.codec.http;

import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.Connection;

public class HttpFrontConnection extends Connection {
	
	public HttpFrontConnection(SocketChannel channel) {
		super(channel);
	}
	
}
