package com.feeyo.redis.net.backend;

import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.Connection;
import com.feeyo.redis.net.backend.callback.BackendCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;

/**
 * 后端连接
 * 
 * @author zhuam
 *
 */
public class BackendConnection extends Connection {
	
	//
	protected BackendCallback callback;
    protected PhysicalNode physicalNode;
    
    protected volatile boolean borrowed = false;
    protected volatile long heartbeatTime = 0;	//心跳应答时间
    
    private volatile long lastTime;
    
	public BackendConnection(SocketChannel socketChannel) {
		
		super(socketChannel);
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

	
	
}