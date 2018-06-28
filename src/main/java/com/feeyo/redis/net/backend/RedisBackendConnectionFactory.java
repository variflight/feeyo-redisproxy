package com.feeyo.redis.net.backend;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.NetSystem;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.callback.BackendCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;

public class RedisBackendConnectionFactory implements BackendConnectionFactory {

	@Override
	public BackendConnection make(PhysicalNode physicalNode,
			BackendCallback callback, Object attachement) throws IOException {
		
		String host = physicalNode.getHost();
		int port = physicalNode.getPort();
		
		SocketChannel channel = SocketChannel.open();
		channel.configureBlocking(false);

		RedisBackendConnection c = new RedisBackendConnection( channel );
		NetSystem.getInstance().setSocketParams(c, false);

		// 设置NIOHandlers
		c.setHandler( new RedisBackendConnectionHandler() );
		c.setNetflowController( RedisEngineCtx.INSTANCE().getNetflowController() );
		
		c.setHost( host );
		c.setPort( port );
		c.setPhysicalNode( physicalNode );
		c.setCallback( callback );
		c.setAttachement( attachement );
		c.setIdleTimeout( NetSystem.getInstance().getNetConfig().getBackendIdleTimeout() );
		
		// 连接 
		NetSystem.getInstance().getConnector().postConnect(c);
		return c;
	}

}
