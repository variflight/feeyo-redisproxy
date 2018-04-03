package com.feeyo.redis.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wuzh
 */
public final class NIOAcceptor extends Thread {
	
	private static Logger LOGGER = LoggerFactory.getLogger( NIOAcceptor.class );
	
	private final int port;
	private final Selector selector;
	private final ServerSocketChannel serverChannel;
	private final ConnectionFactory factory;
	private long acceptCount;
	private final NIOReactorPool reactorPool;

	public NIOAcceptor(String name, String bindIp, int port,
			ConnectionFactory factory, NIOReactorPool reactorPool) throws IOException {
		
		super.setName(name);
		this.port = port;
		this.selector = Selector.open();
		this.serverChannel = ServerSocketChannel.open();
		this.serverChannel.configureBlocking(false);
		
		/** 设置TCP属性 */
		this.serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		this.serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 32); // 32K
		
		// backlog=200
		this.serverChannel.bind(new InetSocketAddress(bindIp, port), 500);
		this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
		this.factory = factory;
		this.reactorPool = reactorPool;
	}

	public int getPort() {
		return port;
	}

	public long getAcceptCount() {
		return acceptCount;
	}

	@Override
	public void run() {
		final Selector selector = this.selector;
		for (;;) {
			++acceptCount;
			try {
				selector.select( 1000L );
				Set<SelectionKey> keys = selector.selectedKeys();
				try {
					for (SelectionKey key : keys) {
						if (key.isValid() && key.isAcceptable()) {
							accept();							
						} else {
							key.cancel();
						}
					}
				} finally {
					keys.clear();
				}
			} catch (Throwable e) {
				LOGGER.warn(getName(), e);
			}
		}
	}

	/**
	 * 接受新连接
	 */
	private void accept() {
		SocketChannel channel = null;
		try {
			channel = serverChannel.accept();
			channel.configureBlocking( false );
			
			// 构建Connection
			Connection c = factory.make(channel);
			c.setDirection( Connection.Direction.in );
			//c.setId( ConnectIdGenerator.getINSTNCE().getId() );
			
			InetSocketAddress remoteAddr = (InetSocketAddress) channel.getRemoteAddress();
			c.setHost(remoteAddr.getHostString());
			c.setPort(remoteAddr.getPort());
			
			// 将新连接派发至reactor进行异步处理
			NIOReactor reactor = reactorPool.getNextReactor();
			reactor.postRegister(c);

		} catch (Exception e) {
			LOGGER.warn(getName(), e);
			closeChannel(channel);
		}
	}

	private static void closeChannel(SocketChannel channel) {
		if (channel == null) {
			return;
		}

		Socket socket = channel.socket();
		if (socket != null) {
			try {
				socket.close();
			} catch (IOException e) {
				 LOGGER.error("closeChannelError", e);
			}
		}

		try {
			channel.close();
		} catch (IOException e) {
			 LOGGER.error("closeChannelError", e);
		}
	}
}