package com.feeyo.net.nio;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.buffer.BufferPool;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;


/**
 * 存放当前所有连接的信息，包括客户端和服务端等，以及Network部分所使用共用对象
 *
 * @author wuzhih
 * @author zhuam
 *
 */
public class NetSystem {
	
	private static Logger LOGGER = LoggerFactory.getLogger( NetSystem.class );
	
	public static final int RUNNING = 0;
	public static final int SHUTING_DOWN = -1;
	
	private static NetSystem INSTANCE;
	private final BufferPool bufferPool;
	
	// 用来执行那些耗时的任务
	private final NameableExecutor businessExecutor;
	
	// 用来执行定时任务
	private final NameableExecutor timerExecutor;
	
	private final int TIMEOUT = 1000 * 60 * 3; //3分钟
	
	private final ConcurrentHashMap<Long, ClosableConnection> allConnections;
	private SystemConfig netConfig;
	private NIOConnector connector;

	public static NetSystem getInstance() {
		return INSTANCE;
	}

	public NetSystem(BufferPool bufferPool,  NameableExecutor businessExecutor, NameableExecutor timerExecutor)
			throws IOException {
		this.bufferPool = bufferPool;
		this.businessExecutor = businessExecutor;
		this.timerExecutor = timerExecutor;
		this.allConnections = new ConcurrentHashMap<Long, ClosableConnection>();
		INSTANCE = this;
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public NIOConnector getConnector() {
		return connector;
	}

	public void setConnector(NIOConnector connector) {
		this.connector = connector;
	}

	public SystemConfig getNetConfig() {
		return netConfig;
	}

	public void setNetConfig(SystemConfig netConfig) {
		this.netConfig = netConfig;
	}

	public NameableExecutor getBusinessExecutor() {
		return businessExecutor;
	}

	public NameableExecutor getTimerExecutor() {
		return timerExecutor;
	}

	/**
	 * 添加一个连接到系统中被监控
	 */
	public void addConnection(ClosableConnection c) {
		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug("add:" + c);
		}
		
		allConnections.put(c.getId(), c);
	}
	
	public void removeConnection(ClosableConnection c) {
		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug("remove:" + c);
		}
		
		this.allConnections.remove( c.getId() );
	}

	public ConcurrentMap<Long, ClosableConnection> getAllConnectios() {
		return allConnections;
	}

	
	
	/**
	 * 定时执行该方法，回收部分资源。
	 */
	public void checkConnections() {
		
		Iterator<Entry<Long, ClosableConnection>> it = allConnections.entrySet().iterator();
		while (it.hasNext()) {
			ClosableConnection c = it.next().getValue();
			// 删除空连接
			if (c == null) {
				it.remove();
				continue;
			}

			// 后端在用连接的超时处理
			if ( c instanceof BackendConnection ) {
				BackendConnection backendCon = (BackendConnection)c;
				if ( backendCon.isBorrowed() ) {
					
					if (  backendCon.getLastTime() < TimeUtil.currentTimeMillis() - TIMEOUT ) {
						
						StringBuffer errSB = new StringBuffer();
						errSB.append("backend timeout, close it" ).append( c );
						errSB.append(" , and attach it " ).append( c.getAttachement() );
						LOGGER.error( errSB.toString() );
						
						c.close("backend timeout");
						
					}  else {
						
						//
						StringBuffer errSB = new StringBuffer();
						errSB.append("backend kill, close it" ).append( c );
						errSB.append(" , and attach it " ).append( c.getAttachement() );
						LOGGER.error( errSB.toString() );
						
						//
						if ( backendCon.getAttachement() != null && backendCon.getAttachement() instanceof RedisFrontConnection) {
							RedisFrontConnection frontCon = (RedisFrontConnection) backendCon.getAttachement();
							if ( frontCon.isClosed() ) {
								c.close("backend kill, because front con is close! ");
							}
						}
					}
				}
			}
			
			// 清理已关闭连接，否则空闲检查。
			if (c.isClosed()) {
				it.remove();
			} else {

				// very important ,for some data maybe not sent
				if ( c.isConnected() ) {
					c.doNextWriteCheck();
				}

				// 闲置的检查
				c.idleCheck();
			}
		}
	}
	
	public void setSocketParams(ClosableConnection con, boolean isFrontChannel) throws IOException {
		int sorcvbuf = 0;
	    int sosndbuf = 0;
	    int soNoDelay = 0;
	    
		if (isFrontChannel) {
			sorcvbuf = this.netConfig.getFrontsocketsorcvbuf();
			sosndbuf = this.netConfig.getFrontsocketsosndbuf();
			soNoDelay = this.netConfig.getFrontSocketNoDelay();
		} else {
			sorcvbuf = this.netConfig.getBacksocketsorcvbuf();
			sosndbuf = this.netConfig.getBacksocketsosndbuf();
			soNoDelay = this.netConfig.getBackSocketNoDelay();
		}
		
		SocketChannel socketChannel = con.getSocketChannel();
		socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, sorcvbuf);
		socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, sosndbuf);
		socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, soNoDelay == 1);
	    socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
	    socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
	}
}