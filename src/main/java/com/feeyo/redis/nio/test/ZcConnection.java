package com.feeyo.redis.nio.test;

import java.nio.channels.SocketChannel;

import com.feeyo.redis.nio.ZeroCopyConnection;

public class ZcConnection extends ZeroCopyConnection {

	public ZcConnection(SocketChannel channel) {
		super(channel);
	}

}
