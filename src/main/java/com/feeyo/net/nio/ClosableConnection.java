package com.feeyo.net.nio;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.util.TimeUtil;

public abstract class ClosableConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( ClosableConnection.class );
	
	//
	public static final byte[] ERR_FLOW_LIMIT = "-ERR netflow problem, the request is cleaned up. \r\n".getBytes();
	
	// 连接的方向，in表示是客户端连接过来的，out表示自己作为客户端去连接对端Sever
	public enum Direction {
		in, out
	}
	
	//
	public static final int STATE_CONNECTING = 0;
	public static final int STATE_CONNECTED = 1;
	public static final int STATE_CLOSING = -1;
	public static final int STATE_CLOSED = -2;
	
	//
	protected static final int OP_NOT_READ = ~SelectionKey.OP_READ;
	protected static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;
	
	// 支持 backend conn 内嵌套 
	//
	protected boolean isChild = false;
	protected ClosableConnection parent = null;
	
	//
	protected Direction direction = Direction.out;

	protected String host;
	protected int port;
	protected int localPort;
	protected long id;
	protected String reactor;
	protected Object attachement;
	protected int state = STATE_CONNECTING;

	// socket
	protected SocketChannel socketChannel;
	protected SelectionKey processKey;

	//
	protected AtomicBoolean isClosed;
	protected boolean isSocketClosed;
	
	//
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected long closeTime;											// debug
	protected String closeReason = null;
	
																		//
	protected long netInCounter;
	protected long netInBytes;											
	protected long netOutCounter;
	protected long netOutBytes;
	
	protected int writeAttempts;
	
	protected long idleTimeout;
	
	@SuppressWarnings("rawtypes")
	protected NIOHandler handler;
	
	protected NetFlowController netflowController;

	public ClosableConnection() {}
	
	public ClosableConnection(SocketChannel socketChannel) {
		this.socketChannel = socketChannel;
		this.isClosed = new AtomicBoolean(false);
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
		this.id = ConnectIdGenerator.getINSTNCE().getId();
	}
	
	//
	public boolean isChild() {
		return isChild;
	}

	public void setChild(boolean isChild) {
		this.isChild = isChild;
	}

	public ClosableConnection getParent() {
		return parent;
	}

	public void setParent(ClosableConnection parent) {
		this.parent = parent;
	}

	
	//
	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}
	

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public long getId() {
		return id;
	}

	public int getLocalPort() {
		return localPort;
	}

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public void setId(long id) {
		this.id = id;
	}

	public boolean isIdleTimeout() {
 		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public long getLastReadTime() {
		return lastReadTime;
	}

	public long getLastWriteTime() {
		return lastWriteTime;
	}
	
	public long getNetInBytes() {
		return netInBytes;
	}
	
	public long getNetInCounter() {
		return netInCounter;
	}

	public long getNetOutBytes() {
		return netOutBytes;
	}

	public long getNetOutCounter() {
		return netOutCounter;
	}
	

	public void setHandler(NIOHandler<? extends ClosableConnection> handler) {
		this.handler = handler;
	}

	@SuppressWarnings("rawtypes")
	public NIOHandler getHandler() {
		return this.handler;
	}
	
	public void setNetflowController(NetFlowController nfc) {
		this.netflowController = nfc;
	}
	
	public NetFlowController getNetflowController() {
		return netflowController;
	}

	public boolean isConnected() {
		boolean isConnected = (this.state != STATE_CONNECTING && state != STATE_CLOSING && state != STATE_CLOSED);
		return isConnected;
	}

	// 
	//
	@SuppressWarnings("unchecked")
	public void close(String reason) {
		//
		if ( !isClosed.get() ) {
			
			closeSocket();
			isClosed.set(true);
			
			this.closeTime = TimeUtil.currentTimeMillis();
			if ( reason != null ) 
				this.closeReason = reason;
			
			this.cleanup();		
			
			if ( isChild )  {
				NetSystem.getInstance().removeConnection( parent );
				if ( handler != null )
					handler.onClosed(parent, reason);
				
			} else {
				
				NetSystem.getInstance().removeConnection(this);
				if ( handler != null )
					handler.onClosed(this, reason);
			}
			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("close connection, reason:" + reason + " ," + this.toString());
			}
			
			this.attachement = null; //help GC
			
		} else {
		    this.cleanup();
		}
	}

	public boolean isClosed() {
		return isClosed.get();
	}

	
	public void idleCheck() {		
		if ( isIdleTimeout() ) {			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug(toString() + " idle timeout");
			}
			close("idle timeout ");
		}
	}

	/**
	 * 清理资源
	 */
	protected abstract void cleanup();
	
	
	private void clearSelectionKey() {
		try {
			SelectionKey key = this.processKey;
			if (key != null && key.isValid()) {
				key.attach(null);
				key.cancel();
			}
		} catch (Exception e) {
			LOGGER.warn("clear selector keys err:" + e);
		}
	}


	@SuppressWarnings("unchecked")
	public void register(Selector selector) throws IOException {
		try {	
			processKey = socketChannel.register(selector, SelectionKey.OP_READ, this);
			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug("register:" + this);
			}
			
			// 已连接、默认不需要认证
	        this.setState( Connection.STATE_CONNECTED );  
			
	        // 支持代理 CON
			if ( isChild ) {
				NetSystem.getInstance().addConnection( parent );
				this.handler.onConnected( parent );
				
			} else {
				NetSystem.getInstance().addConnection(this);
				this.handler.onConnected( this );
			}
			
		} finally {
			if ( isClosed() ) {
				clearSelectionKey();
			}
		}
	}
	
	//
	public abstract void doNextWriteCheck();
	
	public abstract void write(byte[] data);
	public abstract void write(ByteBuffer data);
	

	protected void disableWrite() {
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() & OP_NOT_WRITE);
		} catch (Exception e) {
			LOGGER.warn("can't disable write " + this, e);
		}
	}

	protected void enableWrite(boolean wakeup) {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
			needWakeup = true;
		} catch (Exception e) {
			LOGGER.warn("can't enable write: ", e);
		}
		
		if (needWakeup && wakeup) {
			processKey.selector().wakeup();
		}
	}
	
	public void setReactor(String reactorName) {
		this.reactor = reactorName;
	}

	public String getReactor() {
		return this.reactor;
	}

	public boolean belongsReactor(String reacotr) {
		return reactor.equals(reacotr);
	}

	public Object getAttachement() {
		return attachement;
	}

	public void setAttachement(Object attachement) {
		this.attachement = attachement;
	}

	protected void disableRead() {
		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() & OP_NOT_READ);
	}

	protected void enableRead() {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_READ);
			needWakeup = true;
		} catch (Exception e) {
			LOGGER.warn("enable read fail ", e);
		}
		
		if (needWakeup) {
			processKey.selector().wakeup();
		}
	}

	public void setState(int newState) {
		
		this.state = newState;
	}
	
	/**
	 * 异步读取,该方法在 reactor 中被调用
	 */
	public abstract void asynRead() throws IOException;
	
	
	protected void closeSocket() {
		if ( socketChannel != null ) {		
			
			if (socketChannel instanceof SocketChannel) {
				Socket socket = ((SocketChannel) socketChannel).socket();
				if (socket != null) {
					try {
						socket.close();
					} catch (IOException e) {
						LOGGER.error("closeChannelError", e);
					}
				}
			}			
			
			boolean isSocketClosed = true;
			try {
				processKey.cancel();
				socketChannel.close();
			} catch (Throwable e) {
			}			
			boolean closed = isSocketClosed && (!socketChannel.isOpen());
			if (!closed) {
				LOGGER.warn("close socket of connnection failed " + this);
			}
		}
	}

	public int getState() {
		return state;
	}

	public Direction getDirection() {
		return direction;
	}

	public void setDirection(Direction in) {
		this.direction = in;
	}

	public void flowClean() {
		LOGGER.warn("Flow cleaning,  {}", this );
		// ignore
	}
	
}