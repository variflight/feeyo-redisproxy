package com.feeyo.redis.net.front;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.config.UserCfg;

/**
 * 
 * @author zhuam
 *
 */
public class RedisFrontConnection extends FrontConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( RedisFrontConnection.class );
	
	private static final long AUTH_TIMEOUT = 15 * 1000L;
	
	// 用户配置
	private UserCfg userCfg;
	
	private boolean isAuthenticated;
	
	private RedisFrontSession session;
	
	private AtomicBoolean _readLock = new AtomicBoolean(false);
	
	public RedisFrontConnection(SocketChannel channel) {
		super(channel);
		this.session = new RedisFrontSession( this );
		this.setIdleTimeout( NetSystem.getInstance().getNetConfig().getFrontIdleTimeout() );
	}
	
	public RedisFrontSession getSession() {
		return this.session;
	}
	
	@Override
	public void asynRead() throws IOException {
		
		if (_readLock.compareAndSet(false, true)) {
			super.asynRead();
		}
		
	}
	
	@Override
	public boolean isIdleTimeout() {
		if ( isAuthenticated ) {
			return super.isIdleTimeout();
		} else {
			return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
		}
	}
	
	public boolean isAuthenticated() {
		return isAuthenticated;
	}

	public void setAuthenticated(boolean isAuthenticated) {
		this.isAuthenticated = isAuthenticated;
	}

	public UserCfg getUserCfg() {
		return userCfg;
	}

	public void setUserCfg(UserCfg userCfg) {
		this.userCfg = userCfg;
	}

	@Override
	public void close(String reason) {
		super.close(reason);
	}
	
	public void releaseLock() {
		_readLock.set(false);
	}

	
	@Override
	public void flowClean() {
		
		LOGGER.warn("flow clean, front: {} ", this);
		//
		this.write( ERR_FLOW_LIMIT );
		this.close("flow limit");
	}
	
	
	@Override
	public String toString() {
		StringBuffer sbuffer = new StringBuffer(200);
		sbuffer.append( "Conn [reactor=").append( reactor );
		sbuffer.append(", host=").append( host ).append(":").append( port );
		sbuffer.append(", password=").append( userCfg != null ? userCfg.getPassword() : "no auth!" );	
		sbuffer.append(", id=").append( id );
		
		if ( session != null) {
			sbuffer.append(", cmd=").append( session.getRequestCmd() );
			sbuffer.append(", key=").append( session.getRequestKey() != null ? new String( session.getRequestKey() ) : "" );
		}
		
		sbuffer.append(", readLock=").append(  _readLock.get() );
		sbuffer.append(", startup=").append( TimeUtil.formatTimestamp( startupTime ) );
		sbuffer.append(", lastRT=").append( TimeUtil.formatTimestamp( lastReadTime ) );
		sbuffer.append(", lastWT=").append( TimeUtil.formatTimestamp( lastWriteTime ) );
		sbuffer.append(", attempts=").append( writeAttempts );	//
		
		if ( isClosed.get() ) {
			sbuffer.append(", isClosed=").append( isClosed.get() );
			sbuffer.append(", closeTime=").append( TimeUtil.formatTimestamp( closeTime ) );
			sbuffer.append(", closeReason=").append( closeReason );
		}
		
		sbuffer.append("]");
		return  sbuffer.toString();
	}
	
	
}
