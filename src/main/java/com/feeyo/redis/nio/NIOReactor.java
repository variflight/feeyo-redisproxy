package com.feeyo.redis.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 网络事件反应器
 * 
 * @author wuzh
 * @author zhuam
 */
public final class NIOReactor {
	
	private static Logger LOGGER = LoggerFactory.getLogger( NIOReactor.class );
	
	private final static long SELECTOR_TIMEOUT = 40L; // 500L
	
	private final String name;
	private final RW reactorR;

	public NIOReactor(String name) throws IOException {
		this.name = name;
		this.reactorR = new RW();
	}

	public String getName() {
		return name;
	}

	final void startup() {
		new Thread(reactorR, name + "-RW").start();
	}

	final void postRegister(ClosableConnection c) {
		c.setReactor( this.name );
		reactorR.pendingQueue.offer(c);
		reactorR.selector.wakeup();
	}

	final Queue<ClosableConnection> getRegisterQueue() {
		return reactorR.pendingQueue;
	}

	final long getReactCount() {
		return reactorR.reactCount;
	}

	
	// IO/RW 线程
	private final class RW implements Runnable {
		
		private final Selector selector;
		private final ConcurrentLinkedQueue<ClosableConnection> pendingQueue;
		private long reactCount;
        
		private RW() throws IOException {
			this.selector = Selector.open();
			this.pendingQueue = new ConcurrentLinkedQueue<ClosableConnection>();  
		}

		@Override
		public void run() {
			
			final Selector selector = this.selector;
			long ioTimes = 0;
			
			for (;;) {
				++reactCount;
				try {

					// 查看有无连接就绪
					selector.select( SELECTOR_TIMEOUT );

					final Set<SelectionKey> keys = selector.selectedKeys();
					if ( keys.isEmpty() ) {
						if (!pendingQueue.isEmpty()) {
							ioTimes = 0;
							processPendingQueue(selector); 	// 处理注册队列
						}
						continue;
						
					} else if ((ioTimes > 5) & !pendingQueue.isEmpty()) {
						
						ioTimes = 0;
						processPendingQueue(selector); 		// 处理注册队列
					}
					

					ioTimes++;
					for (final SelectionKey key : keys) {

						ClosableConnection con = null;
						
						try {
							
							Object att = key.attachment();
							if ( att != null ) {
								
								con = (ClosableConnection) att;
								
								int ops = key.readyOps();
								
								// see ACE, first write,  Accept > Write > Read
								
								// 处理写
								if ( (ops & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE ) {
									con.doNextWriteCheck();
								}
								
								// 处理读
								if ( (ops & SelectionKey.OP_READ) == SelectionKey.OP_READ ) {									
									try {
										con.asynRead();
									} catch (IOException e) {
										con.close("program err:" + e.toString());										 
										continue;
										
									} catch (Exception e) {
										LOGGER.warn("caught err:", e);
										con.close("program err:" + e.toString());
										continue;
									}
								}								
								
							} else {
								key.cancel();
							}

						} catch (final Throwable e) {
							
							key.cancel();
							
							// Catch exceptions such as OOM and close connection if exists
                        	//so that the reactor can keep running!
							if (con != null) {
								con.close("Bad: " + e);
							}
							
							LOGGER.error("caught err: ", e);
						}
					}
					
					keys.clear();
					
				} catch (Throwable e) {
					// Catch exceptions such as OOM so that the reactor can keep running!
					LOGGER.error(name +" caught err: ", e);
				} 
			}
		}
		
		// 注册 IO 读写事件
		private void processPendingQueue(Selector selector) {
			ClosableConnection c = null;
			while ((c = pendingQueue.poll()) != null) {
				try {
					c.register(selector);
				} catch (Exception e) {
					//LOGGER.warn("register error ", e);
					c.close("register err");
				}
			}
		}
	}
}