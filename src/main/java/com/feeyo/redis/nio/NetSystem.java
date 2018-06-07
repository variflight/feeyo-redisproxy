package com.feeyo.redis.nio;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.nio.buffer.BufferPool;
import com.feeyo.redis.nio.util.TimeUtil;


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
	
	private final int TIMEOUT = 1000 * 60 * 5; //5分钟
	
	private final ConcurrentHashMap<Long, AbstractConnection> allConnections;
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
		this.allConnections = new ConcurrentHashMap<Long, AbstractConnection>();
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
	public void addConnection(AbstractConnection c) {
		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug("add:" + c);
		}
		
		allConnections.put(c.getId(), c);
	}
	
	public void removeConnection(AbstractConnection c) {
		
		if ( LOGGER.isDebugEnabled() ) {
			LOGGER.debug("remove:" + c);
		}
		
		this.allConnections.remove( c.getId() );
	}

	public ConcurrentMap<Long, AbstractConnection> getAllConnectios() {
		return allConnections;
	}

	
	
	/**
	 * 定时执行该方法，回收部分资源。
	 */
	public void checkConnections() {
		Iterator<Entry<Long, AbstractConnection>> it = allConnections.entrySet().iterator();
		while (it.hasNext()) {
			AbstractConnection c = it.next().getValue();
			// 删除空连接
			if (c == null) {
				it.remove();
				continue;
			}

			// 后端超时的连接关闭
			if ( c instanceof BackendConnection ) {
				BackendConnection backendCon = (BackendConnection)c;
				if (backendCon.isBorrowed() && backendCon.getLastTime() < TimeUtil.currentTimeMillis() - TIMEOUT ) {
					
					StringBuffer errBuffer = new StringBuffer();
					errBuffer.append("backend timeout, close it " ).append( c );
					if ( c.getAttachement() != null ) {
						errBuffer.append(" , and attach it " ).append( c.getAttachement() );
					}
					errBuffer.append( " , channel isConnected: " ).append(backendCon.getSocketChannel().isConnected());
			        errBuffer.append( " , channel isBlocking: " ).append(backendCon.getSocketChannel().isBlocking());
			        errBuffer.append( " , channel isOpen: " ).append(backendCon.getSocketChannel().isOpen());
			        errBuffer.append( " , socket isConnected: " ).append(backendCon.getSocketChannel().socket().isConnected());
			        errBuffer.append( " , socket isClosed: " ).append(backendCon.getSocketChannel().socket().isClosed());
			        
					LOGGER.warn( errBuffer.toString() );
					
					c.close("backend timeout");
				}
			}
			
			// 清理已关闭连接，否则空闲检查。
			if (c.isClosed()) {
				it.remove();
			} else {

				// very important ,for some data maybe not sent
				if ( c.isConnected() && !c.writeQueue.isEmpty() ) {
					c.doNextWriteCheck();
				}

				c.idleCheck();
			}
		}
	}
	
	public void setSocketParams(AbstractConnection con, boolean isFrontChannel) throws IOException {
		//int sorcvbuf = 0;
		int sosndbuf = 0;
		
		if (isFrontChannel) {
			//sorcvbuf = netConfig.getFrontsocketsorcvbuf();
			sosndbuf = netConfig.getFrontsocketsosndbuf();
		} else {
			//sorcvbuf = netConfig.getBacksocketsorcvbuf();
			sosndbuf = netConfig.getBacksocketsosndbuf();
		}
		
		// LINUX 2.6 该 RCVBUF 会自动调节
		// con.getChannel().socket().setReceiveBufferSize( sorcvbuf );
		con.getSocketChannel().socket().setSendBufferSize( sosndbuf  );  // SO_TCPNODELAY  关闭算法
		con.getSocketChannel().socket().setTcpNoDelay( true );
		con.getSocketChannel().socket().setKeepAlive( true );
		con.getSocketChannel().socket().setReuseAddress( true );
	}
}