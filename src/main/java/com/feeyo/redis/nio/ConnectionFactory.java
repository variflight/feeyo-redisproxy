package com.feeyo.redis.nio;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * @author wuzh
 */
public abstract class ConnectionFactory {

	public abstract AbstractConnection make(SocketChannel channel) throws IOException;
	
}
