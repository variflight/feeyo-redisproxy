package com.feeyo.redis.nio;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
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

	final void postRegister(Connection c) {
		c.setReactor( this.name );
		reactorR.registerQueue.offer(c);
		reactorR.selector.wakeup();
	}

	final Queue<Connection> getRegisterQueue() {
		return reactorR.registerQueue;
	}

	final long getReactCount() {
		return reactorR.reactCount;
	}

	
	// IO/RW 线程
	private final class RW implements Runnable {
		
		private final Selector selector;
		private final ConcurrentLinkedQueue<Connection> registerQueue;
		private long reactCount;
        
		private RW() throws IOException {
			this.selector = Selector.open();
			this.registerQueue = new ConcurrentLinkedQueue<Connection>();  
		}

		@Override
		public void run() {
			
			final Selector selector = this.selector;
			Set<SelectionKey> keys = null;
			for (;;) {
				++reactCount;
				try {
					
					// 查看有无连接就绪
					selector.select(500L);
					
					// 处理注册队列
					register(selector);
					
					keys = selector.selectedKeys();
					for (final SelectionKey key : keys) {
						
						Connection con = null;
						try {
							Object att = key.attachment();
							if ( att != null ) {
								
								con = (Connection) att;
								
								// 处理读
								if (key.isValid() && key.isReadable()) {									
									try {
										con.asynRead();
									} catch (IOException  e) {
										con.close("program err:" + e.toString());										 
										continue;
										
									} catch (Exception e) {
										LOGGER.warn("caught err:", e);
										con.close("program err:" + e.toString());
										continue;
									}
								}								
								
								// 处理写
								if (key.isValid() && key.isWritable()) {
									con.doNextWriteCheck();
								}
								
							} else {
								key.cancel();
							}
							
						} catch (CancelledKeyException e) {			
							
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug(con + " socket key canceled");
							}
							
						} catch (Exception e) {
							LOGGER.warn(con + " " + e);
							
						} catch (final Throwable e) {
							// Catch exceptions such as OOM and close connection if exists
                        	//so that the reactor can keep running!
							if (con != null) {
								con.close("Bad: " + e);
							}
							LOGGER.error("caught err: ", e);
                        	continue;
						}
					}
					
					
					
				} catch (Exception e) {
					LOGGER.error(name, e);
					
				} catch (Throwable e) {
					// Catch exceptions such as OOM so that the reactor can keep running!
					LOGGER.error(name +" caught err: ", e);
				} finally {
					if (keys != null) {
						keys.clear();
					}
				}
			}
		}
		
		// 注册 IO 读写事件
		private void register(Selector selector) {
			if ( registerQueue.isEmpty() ) {
				return;
			}
			
			Connection c = null;
			while ((c = registerQueue.poll()) != null) {
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