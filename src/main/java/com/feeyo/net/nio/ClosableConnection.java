package com.feeyo.net.nio;

import java.io.IOException;
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
	
	//																//
	protected long netInBytes;											
	protected long netOutBytes;
	protected int writeAttempts;
	
	protected long idleTimeout;
	
	@SuppressWarnings("rawtypes")
	protected NIOHandler handler;


	public ClosableConnection(SocketChannel socketChannel) {
		this.socketChannel = socketChannel;
		this.isClosed = new AtomicBoolean(false);
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
		this.id = ConnectIdGenerator.getINSTNCE().getId();
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
	
	public long getNetOutBytes() {
		return netOutBytes;
	}
	

	public void setHandler(NIOHandler<? extends ClosableConnection> handler) {
		this.handler = handler;
	}

	@SuppressWarnings("rawtypes")
	public NIOHandler getHandler() {
		return this.handler;
	}

	public boolean isConnected() {
		boolean isConnected = (this.state != STATE_CONNECTING && state != STATE_CLOSING && state != STATE_CLOSED);
		return isConnected;
	}

	public boolean isClosed() {
		return isClosed.get();
	}

	public boolean isIdleTimeout() {
 		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
	}

	public void idleCheck() {		
		if ( isIdleTimeout() ) {			
			if ( LOGGER.isDebugEnabled() ) {
				LOGGER.debug(toString() + " idle timeout");
			}
			close("idle timeout ");
		}
	}
	
	//
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

	public void setState(int newState) {
		this.state = newState;
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
	
	//
	// ______________________________________________________________________________________
	
	// Accept  register
	public abstract void register(Selector selector) throws IOException;
	
	// 异步读取,该方法在 reactor 中被调用
	public abstract void asynRead() throws IOException;
	
	//
	public abstract void doNextWriteCheck();
	public abstract void write(byte[] data);
	public abstract void write(ByteBuffer data);
	
	// 
	public abstract void close(String reason);
	
	// Network Flow Guard (NFG) 
	protected boolean flowGuard(long length) {
		return false;
		// ignore
	}
	
	
	//
	// ______________________________________________________________________________________
	//
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
	
}