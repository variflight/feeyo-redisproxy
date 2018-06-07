package com.feeyo.redis.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NIO 连接器，用于连接对方Server
 * 
 * @author wuzh
 */
public final class NIOConnector extends Thread {
	
	private static Logger LOGGER = LoggerFactory.getLogger( NIOConnector.class );
	
	private final String name;
	private final Selector selector;
	private final BlockingQueue<AbstractConnection> connectQueue;
	private long connectCount;
	private final NIOReactorPool reactorPool;

	public NIOConnector(String name, NIOReactorPool reactorPool) throws IOException {
		super.setName(name);
		this.name = name;
		this.selector = Selector.open();
		this.reactorPool = reactorPool;
		this.connectQueue = new LinkedBlockingQueue<AbstractConnection>();
	}

	public long getConnectCount() {
		return connectCount;
	}

	/**
	 * 添加一个需要异步连接的Connection到队列中，等待连接
	 * 
	 * @param AbstractConnection
	 */
	public void postConnect(AbstractConnection c) {
		connectQueue.offer(c);
		selector.wakeup();
	}

	@Override
	public void run() {
		final Selector selector = this.selector;
		for (;;) {
			++connectCount;
			try {
				//查看有无连接就绪
				selector.select( 1000L );
				connect(selector);
				Set<SelectionKey> keys = selector.selectedKeys();
				try {
					for (SelectionKey key: keys) {
						Object att = key.attachment();
						if (att != null && key.isValid() && key.isConnectable()) {
							finishConnect(key, att);
						} else {
							key.cancel();
						}
					}
				} finally {
					keys.clear();
				}
			} catch (Exception e) {
				LOGGER.warn(name, e);
			}
		}
	}


	private void connect(Selector selector) {
		AbstractConnection c = null;
		while ((c = connectQueue.poll()) != null) {
			try {
				SocketChannel channel = (SocketChannel) c.getSocketChannel();
				//注册OP_CONNECT监听与后端连接是否真正建立
				channel.register(selector, SelectionKey.OP_CONNECT, c);
				 //主动连接
				channel.connect(new InetSocketAddress(c.host, c.port));
			} catch (Exception e) {
				LOGGER.error("error:", e);
				c.close("connect failed:" + e.toString());
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void finishConnect(SelectionKey key, Object att) {
		AbstractConnection c = (AbstractConnection) att;
		try {
			 //做原生NIO连接是否完成的判断和操作
			if (finishConnect(c, (SocketChannel) c.socketChannel)) {
				clearSelectionKey(key);
				//c.setId( ConnectIdGenerator.getINSTNCE().getId() );
				
				//与特定NIOReactor绑定监听读写
				NIOReactor reactor = reactorPool.getNextReactor();
				reactor.postRegister(c);
			}
		} catch (Throwable e) {
			
			String host = "";
			int port = 0;
			if (c != null) {
				host = c.getHost();
				port = c.getPort();
			}
			LOGGER.warn("caught err : host={}, port={}, Exception={}", new Object[] {host, port, e});
			
			//异常, 将key清空
			clearSelectionKey(key);
			c.close(e.toString());
			c.getHandler().onConnectFailed(c, new Exception(e.getMessage()));

		}
	}

	private boolean finishConnect(AbstractConnection c, SocketChannel channel) throws IOException {
		if (channel.isConnectionPending()) {
			channel.finishConnect();
			c.setLocalPort(channel.socket().getLocalPort());
			return true;
		} else {
			return false;
		}
	}

	private void clearSelectionKey(SelectionKey key) {
		if (key.isValid()) {
			key.attach(null);
			key.cancel();
		}
	}
}