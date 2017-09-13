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

import com.feeyo.redis.nio.util.SelectorUtil;

/**
 * NIO 连接器，用于连接对方Server
 * 
 * @author wuzh
 */
public final class NIOConnector extends Thread {
	
	private static Logger LOGGER = LoggerFactory.getLogger( NIOConnector.class );
	
	private final String name;
	private volatile Selector selector;
	private final BlockingQueue<Connection> connectQueue;
	private long connectCount;
	private final NIOReactorPool reactorPool;

	public NIOConnector(String name, NIOReactorPool reactorPool) throws IOException {
		super.setName(name);
		this.name = name;
		this.selector = SelectorUtil.openSelector();
		this.reactorPool = reactorPool;
		this.connectQueue = new LinkedBlockingQueue<Connection>();
	}

	public long getConnectCount() {
		return connectCount;
	}

	/**
	 * 添加一个需要异步连接的Connection到队列中，等待连接
	 * 
	 * @param Connection
	 */
	public void postConnect(Connection c) {
		connectQueue.offer(c);
		selector.wakeup();
	}

	@Override
	public void run() {
		
		int invalidSelectCount = 0;
		for (;;) {
			final Selector tSelector = this.selector;
			++connectCount;
			try {
				
				long start = System.nanoTime();
				tSelector.select(1000L);				//查看有无连接就绪
				long end = System.nanoTime();
				connect(tSelector);
				Set<SelectionKey> keys = tSelector.selectedKeys();
				if (keys.size() == 0 && (end - start) < SelectorUtil.MIN_SELECT_TIME_IN_NANO_SECONDS) {
					invalidSelectCount++;
				} else {
					
					try {
						for (SelectionKey key : keys) {
							Object att = key.attachment();
							if (att != null && key.isValid() && key.isConnectable()) {
								finishConnect(key, att);
							} else {
								key.cancel();
							}
						}
					} finally {
						invalidSelectCount = 0;
						keys.clear();
					}
					
				}
				
				if (invalidSelectCount > SelectorUtil.REBUILD_COUNT_THRESHOLD) {
					final Selector rebuildSelector = SelectorUtil.rebuildSelector(this.selector);
					if (rebuildSelector != null) {
						this.selector = rebuildSelector;
					}
					invalidSelectCount = 0;
				}
				
			} catch (Exception e) {
				LOGGER.warn(name, e);
			}
		}
	}


	private void connect(Selector selector) {
		Connection c = null;
		while ((c = connectQueue.poll()) != null) {
			try {
				SocketChannel channel = (SocketChannel) c.getChannel();
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
		Connection c = (Connection) att;
		try {
				
			 //做原生NIO连接是否完成的判断和操作
			 SocketChannel channel = (SocketChannel) c.channel;
			 if (channel.isConnectionPending()) {
				 channel.finishConnect();
				 c.setLocalPort(channel.socket().getLocalPort());
				 
				 clearSelectionKey(key);
				//c.setId( ConnectIdGenerator.getINSTNCE().getId() );
				 
				 //与特定NIOReactor绑定监听读写
				 NIOReactor reactor = reactorPool.getNextReactor();
				 reactor.postRegister(c);
			 }
			
		} catch (Throwable e) {
			
			LOGGER.warn("caught err : conn={}", c, e);
			
			//异常, 将key清空
			clearSelectionKey(key);
			c.close(e.toString());
			c.getHandler().onConnectFailed(c, new Exception(e.getMessage()));
		}
	}
	
	private void clearSelectionKey(SelectionKey key) {
		if (key.isValid()) {
			key.attach(null);
			key.cancel();
		}
	}
}