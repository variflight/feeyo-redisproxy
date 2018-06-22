package com.feeyo.net.codec.protobuf.test;

import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.Connection;

public class ProtobufFrontConnection extends Connection{

	public ProtobufFrontConnection(SocketChannel socketChannel) {
		super(socketChannel);
	}
	
}
