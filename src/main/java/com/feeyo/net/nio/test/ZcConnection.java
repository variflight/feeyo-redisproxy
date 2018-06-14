package com.feeyo.net.nio.test;

import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.ZeroCopyConnection;

public class ZcConnection extends ZeroCopyConnection {

	public ZcConnection(SocketChannel channel) {
		super(channel);
	}

}
