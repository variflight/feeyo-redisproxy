package com.feeyo.redis.net.backend;

import java.nio.channels.SocketChannel;

import com.feeyo.redis.net.Connection;
import com.feeyo.redis.net.backend.callback.BackendCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.nio.util.TimeUtil;

/**
 * 后端连接
 * 
 * @author zhuam
 *
 */
public class BackendConnection extends Connection {
	
	protected BackendCallback callback;
    protected PhysicalNode physicalNode;
    
    protected volatile boolean borrowed = false;
    
    protected volatile long heartbeatTime = 0;	//心跳应答时间
    
	public BackendConnection(SocketChannel channel) {
		super(channel);
	}
	
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

	
	@Override
	public void close(String reason) {
		super.close(reason);
	}

	public long getHeartbeatTime() {
		return heartbeatTime;
	}

	public void setHeartbeatTime(long heartbeatTime) {
		this.heartbeatTime = heartbeatTime;
	}

	@Override
	public String toString() {
		StringBuffer sbuffer = new StringBuffer(100);
		sbuffer.append( "Con [reactor=").append( reactor );
		sbuffer.append(", host=").append( host ).append("/").append( port );
		sbuffer.append(", id=").append( id );
		sbuffer.append(", borrowed=").append( borrowed );
		sbuffer.append(", startup=").append( TimeUtil.formatTimestamp(startupTime) );
		sbuffer.append(", lastRT=").append( TimeUtil.formatTimestamp(lastReadTime) );
		sbuffer.append(", lastWT=").append( TimeUtil.formatTimestamp(lastWriteTime) );
		sbuffer.append(", attempts=").append( writeAttempts );	//
		sbuffer.append(", counter=").append( netInCounter ).append("/").append( netOutCounter );	//
		if ( heartbeatTime > 0 ) {
			sbuffer.append(", HT=").append( TimeUtil.formatTimestamp(heartbeatTime) );
		}
		
		if ( isClosed.get() ) {
			sbuffer.append(", isClosed=").append( isClosed );
			sbuffer.append(", closedTime=").append( TimeUtil.formatTimestamp( closeTime) );
			sbuffer.append(", closeReason=").append( closeReason );
		}
		
		sbuffer.append("]");
		return  sbuffer.toString();
	}
	
}
