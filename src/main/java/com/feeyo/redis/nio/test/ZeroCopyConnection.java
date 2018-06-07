package com.feeyo.redis.nio.test;

import java.nio.channels.SocketChannel;

import com.feeyo.redis.nio.AbstractZeroCopyConnection;

public class ZeroCopyConnection extends AbstractZeroCopyConnection {

	public ZeroCopyConnection(SocketChannel channel) {
		super(channel);
	}

}
