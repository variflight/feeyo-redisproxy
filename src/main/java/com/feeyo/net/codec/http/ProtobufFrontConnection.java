package com.feeyo.net.codec.http;

import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.Connection;

public class ProtobufFrontConnection extends Connection {
	
	public ProtobufFrontConnection(SocketChannel channel) {
		super(channel);
	}
	
}
