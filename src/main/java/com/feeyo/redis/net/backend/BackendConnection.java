package com.feeyo.redis.net.backend;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.ClosableConnection;
import com.feeyo.net.nio.Connection;
import com.feeyo.net.nio.NIOHandler;
import com.feeyo.net.nio.NetFlowController;
import com.feeyo.net.nio.ZeroCopyConnection;
import com.feeyo.redis.net.backend.callback.BackendCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;

/**
 * 后端连接
 * 
 * @author zhuam
 *
 */
public class BackendConnection extends ClosableConnection {
	
	// Delegate connection
	//
	private ClosableConnection delegateConn;
	private boolean isZeroCopy = false;

	//
	protected BackendCallback callback;
    protected PhysicalNode physicalNode;
    
    protected volatile boolean borrowed = false;
    protected volatile long heartbeatTime = 0;	//心跳应答时间
    
    private volatile long lastTime;
    
	public BackendConnection(boolean isZeroCopy, SocketChannel socketChannel) {

		if ( isZeroCopy ) {
			delegateConn = new ZeroCopyConnection(socketChannel); 
		} else {
			delegateConn = new Connection(socketChannel);
		}
		
		delegateConn.setParent( this );
		delegateConn.setNested( true );
		this.isZeroCopy = isZeroCopy;
	}
	
	// 
	//
	//
	public BackendCallback getCallback() {
		return callback;
	}

	public void setCallback(BackendCallback callback) {
		this.callback = callback;
	}

	public PhysicalNode getPhysicalNode() {
		return physicalNode;
	}

	public void setPhysicalNode(PhysicalNode node) {
		this.physicalNode = node;
	}
	
	public void release() {
		this.setBorrowed( false );
		this.physicalNode.releaseConnection(this);
	}

	public void setBorrowed(boolean borrowed) {
		this.borrowed = borrowed;
	}
	
	public boolean isBorrowed() {
        return this.borrowed;
    }

	public long getHeartbeatTime() {
		return heartbeatTime;
	}

	public void setHeartbeatTime(long heartbeatTime) {
		this.heartbeatTime = heartbeatTime;
	}
	
	public long getLastTime() {
		return lastTime;
	}

	public void setLastTime(long currentTimeMillis) {
		this.lastTime = currentTimeMillis;
	}

	
	// delegate
	//
	//
	
	@Override
	public long getIdleTimeout() {
		return delegateConn.getIdleTimeout();
	}

	@Override
	public void setIdleTimeout(long idleTimeout) {
		delegateConn.setIdleTimeout(idleTimeout); 
	}
	
	@Override
	public String getHost() {
		return delegateConn.getHost();
	}

	@Override
	public void setHost(String host) {
		delegateConn.setHost(host);
	}

	@Override
	public int getPort() {
		return delegateConn.getPort();
	}

	@Override
	public void setPort(int port) {
		delegateConn.setPort(port);
	}

	@Override
	public long getId() {
		return delegateConn.getId();
	}

	@Override
	public int getLocalPort() {
		return delegateConn.getLocalPort();
	}

	@Override
	public void setLocalPort(int localPort) {
		delegateConn.setLocalPort(localPort);
	}

	@Override
	public void setId(long id) {
		delegateConn.setId(id);
	}

	@Override
	public boolean isIdleTimeout() {
		return delegateConn.isIdleTimeout();
	}

	@Override
	public SocketChannel getSocketChannel() {
		return delegateConn.getSocketChannel();
	}

	@Override
	public long getStartupTime() {
		return delegateConn.getStartupTime();
	}

	@Override
	public long getLastReadTime() {
		return delegateConn.getLastReadTime();
	}

	@Override
	public long getLastWriteTime() {
		return delegateConn.getLastWriteTime();
	}
	
	@Override
	public long getNetInBytes() {
		return delegateConn.getNetInBytes();
	}
	
	@Override
	public long getNetInCounter() {
		return delegateConn.getNetInCounter();
	}

	@Override
	public long getNetOutBytes() {
		return delegateConn.getNetOutBytes();
	}

	@Override
	public long getNetOutCounter() {
		return delegateConn.getNetOutCounter();
	}
	
	@Override
	public void setHandler(NIOHandler<? extends ClosableConnection> handler) {
		this.delegateConn.setHandler(handler);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public NIOHandler getHandler() {
		return delegateConn.getHandler();
	}
	
	@Override
	public void setNetflowController(NetFlowController nfm) {
		delegateConn.setNetflowController(nfm);
	}

	@Override
	public boolean isConnected() {
		return delegateConn.isConnected();
	}

	// 
	//
	@Override
	public void close(String reason) {
		delegateConn.close(reason);
		
		// clear
		delegateConn.setNested( false );
		delegateConn.setParent( null );
	}

	@Override
	public boolean isClosed() {
		return delegateConn.isClosed();
	}

	@Override
	public void idleCheck() {		
		delegateConn.idleCheck();
	}

	/**
	 * 清理资源
	 */
	@Override
	protected void cleanup() {
		// ignore
	}

	@Override
	public void register(Selector selector) throws IOException {
		delegateConn.register(selector);
	}
	
	@Override
	public void doNextWriteCheck() {
		delegateConn.doNextWriteCheck();
	}
	
	@Override
	public void write(byte[] data) {
		delegateConn.write(data);
	}
	
	@Override
	public void write(ByteBuffer data) {
		delegateConn.write(data);
	}
	
	@Override
	public void setReactor(String reactorName) {
		delegateConn.setReactor(reactorName);
	}

	@Override
	public String getReactor() {
		return delegateConn.getReactor();
	}

	@Override
	public boolean belongsReactor(String reacotr) {
		return delegateConn.belongsReactor(reacotr);
	}

	@Override
	public Object getAttachement() {
		return delegateConn.getAttachement();
	}

	@Override
	public void setAttachement(Object attachement) {
		delegateConn.setAttachement(attachement);
	}

	@Override
	public void setState(int newState) {
		delegateConn.setState(newState);
	}
	
	// 异步读取,该方法在 reactor 中被调用
	@Override
	public void asynRead() throws IOException {
		delegateConn.asynRead();
	}
	

	@Override
	public int getState() {
		return delegateConn.getState();
	}

	@Override
	public Direction getDirection() {
		return delegateConn.getDirection();
	}

	@Override
	public void setDirection(Direction in) {
		delegateConn.setDirection(in);
	}

	@Override
	public void flowClean() {
		delegateConn.flowClean();
	}
	
	//
	@Override
	public String toString() {
		StringBuffer sbuffer = new StringBuffer(100);
		sbuffer.append( delegateConn.toString() );
		
		sbuffer.append(", ext[borrowed=").append( borrowed );
		sbuffer.append(", isZeroCopy=").append( isZeroCopy );
		
		if ( heartbeatTime > 0 ) {
			sbuffer.append(", HT=").append( heartbeatTime );
		}
		sbuffer.append("]");

		return  sbuffer.toString();
	}
	
	@Override
	public NetFlowController getNetflowController() {
		return delegateConn.getNetflowController();
	}
	
}