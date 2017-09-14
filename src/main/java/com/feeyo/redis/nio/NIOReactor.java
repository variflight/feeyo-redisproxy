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

import com.feeyo.redis.nio.util.SelectorUtil;

/**
 * 网络事件反应器
 * 
 * @author wuzh
 * @author zhuam
 */
public final class NIOReactor {
	
	private static Logger LOGGER = LoggerFactory.getLogger( NIOReactor.class );
	
	private final String name;
	private final RWThread reactorR;

	public NIOReactor(String name) throws IOException {
		this.name = name;
		this.reactorR = new RWThread(name + "-RW");
	}

	public String getName() {
		return name;
	}

	final void startup() {
		reactorR.start();
	}

	final void postRegister(Connection c) { 
		c.setReactor( this.name );
		this.reactorR.registerQueue.offer(c);
		this.reactorR.selector.wakeup();
	}

	final Queue<Connection> getRegisterQueue() {
		return reactorR.registerQueue;
	}

	final long getReactCount() {
		return reactorR.reactCount;
	}

	
	// IO/RW 线程
	private final class RWThread extends Thread {
		
		//修正NIOReactor局部Selector变量，设置为final防止老化访问之前已经关闭的Selector  //volatile
		private volatile Selector selector;
		private final ConcurrentLinkedQueue<Connection> registerQueue;
		private long reactCount;
		
		private RWThread(String name) throws IOException {
			this.setName(name);
			this.selector = Selector.open();
			this.registerQueue = new ConcurrentLinkedQueue<Connection>();
		}

		@Override
		public void run() {
			
			int invalidSelectCount = 0;
			Set<SelectionKey> keys = null;
			for (;;) {
				
				++reactCount;
				
				try {
					
					final Selector tSelector = this.selector;
					
					long start = System.nanoTime();
					// 查看有无连接就绪
					tSelector.select(500L);
					long end = System.nanoTime();
					
					// 处理注册队列
					register(tSelector);
					
					keys = tSelector.selectedKeys();
					if (keys.isEmpty() && (end - start) < SelectorUtil.MIN_SELECT_TIME_IN_NANO_SECONDS) {
						invalidSelectCount++;
					} else {
						
						invalidSelectCount = 0;
						for (SelectionKey key : keys) {
							Connection con = null;
							try {
								
								Object att = key.attachment();
								if (att != null) {
									con = (Connection) att;
									
									// 处理读
									if (key.isValid() && key.isReadable()) {
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
					}
					
					
					if (invalidSelectCount > SelectorUtil.REBUILD_COUNT_THRESHOLD) {
						final Selector rebuildSelector = SelectorUtil.rebuildSelector( this.selector );
						if (rebuildSelector != null) {
							this.selector = rebuildSelector;
						}
						invalidSelectCount = 0;
					}
					
				} catch (Exception e) {
					LOGGER.warn(name, e);
				} catch (final Throwable e){
					// Catch exceptions such as OOM so that the reactor can keep running!
					LOGGER.error("caught err: ", e);
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
					c.register( selector );
				} catch (Throwable e) {
					LOGGER.warn("register error ", e);
					c.close("register err");
				}
			}
		}
	}
}