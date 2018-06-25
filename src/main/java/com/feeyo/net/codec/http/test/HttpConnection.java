package com.feeyo.net.codec.http.test;

import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.Connection;

public class HttpConnection extends Connection {

	public HttpConnection(SocketChannel socketChannel) {
		super(socketChannel);
	}
	
}
