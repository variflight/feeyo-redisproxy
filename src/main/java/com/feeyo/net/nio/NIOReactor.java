package com.feeyo.net.nio;

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
		
		private volatile Selector selector;
		private final ConcurrentLinkedQueue<ClosableConnection> pendingQueue;
		private long reactCount;
        
		private RW() throws IOException {
			this.selector = Selector.open();
			this.pendingQueue = new ConcurrentLinkedQueue<ClosableConnection>();  
		}

		@Override
		public void run() {
			
			Set<SelectionKey> keys = null;
			
			for (;;) {
				
				++reactCount;
				try {

					Selector selector = this.selector;
					
					// 查看有无连接就绪
					selector.select(500L); // 500L
					
					// 处理注册队列
					processPendingQueue( selector );
					
					keys = selector.selectedKeys();
					for (final SelectionKey key : keys) {
						//
						ClosableConnection con = null;
						try {
							//
							Object att = key.attachment();
							if ( att != null ) {
								
								con = (ClosableConnection) att;
								
								int ops = key.readyOps();
								
								// 1、first write,  Accept > Write > Read  ， ACE
								// 2、first read, Accept > Read > Write	 ， NETTY 
								// 
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
								
								// 处理写
								if ( (ops & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE ) {
									con.doNextWriteCheck();
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
					
				} catch (Throwable e) {
					// Catch exceptions such as OOM so that the reactor can keep running!
					LOGGER.error(name +" caught err: ", e);
					
				} finally {
					if ( keys != null ) {
						keys.clear();
					}
				}
			}
		}
		
		// 注册 IO 读写事件
		private void processPendingQueue(Selector selector) {
			
			if (pendingQueue.isEmpty()) {
				return;
			}
			
			ClosableConnection c = null;
			while ((c = pendingQueue.poll()) != null) {
				try {
					c.register(selector);
				} catch (Exception e) {
					LOGGER.warn("register error ", e);
					c.close("register err");
				}
			}
		}
	}
}