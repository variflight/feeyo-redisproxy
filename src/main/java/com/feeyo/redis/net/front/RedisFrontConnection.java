package com.feeyo.redis.net.front;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.nio.NetSystem;
import com.feeyo.redis.nio.util.TimeUtil;

/**
 * 
 * @author zhuam
 *
 */
public class RedisFrontConnection extends FrontConnection {
	
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
		
		// 流量超标，执行流量清洗
		if ( netFlowMonitor != null && netFlowMonitor.isOverproof() && isFlowLimit()) {
			flowClean();
			return;
		}
		
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
	
	public String toSampleString() {
		StringBuffer sbuffer = new StringBuffer(100);
		sbuffer.append( "RedisFrontConnection [ " );
		sbuffer.append(" reactor=").append( reactor );
		sbuffer.append(", host=").append( host );
		sbuffer.append(", port=").append( port );
		sbuffer.append(", id=").append( id );
		sbuffer.append(", isClosed=").append( isClosed );
		sbuffer.append(", state=").append( state );
		sbuffer.append("]");
		return  sbuffer.toString();
	}
	
	@Override
	public String toString() {
		StringBuffer sbuffer = new StringBuffer(200);
		sbuffer.append( "Connection [reactor=").append( reactor );
		sbuffer.append(", host=").append( host ).append("/").append( port );
		sbuffer.append(", password=").append( userCfg != null ? userCfg.getPassword() : "no auth!" );	
		sbuffer.append(", id=").append( id );
		
		if ( session != null) {
			sbuffer.append(", cmd=").append( session.getRequestCmd() );
			sbuffer.append(", key=").append( session.getRequestKey() != null ? new String( session.getRequestKey() ) : "" );
		}
		
		sbuffer.append(", readLock=").append(  _readLock.get() );
		sbuffer.append(", startupTime=").append( TimeUtil.formatTimestamp( startupTime ) );
		sbuffer.append(", lastReadTime=").append( TimeUtil.formatTimestamp( lastReadTime ) );
		sbuffer.append(", lastWriteTime=").append( TimeUtil.formatTimestamp( lastWriteTime ) );
		sbuffer.append(", writeAttempts=").append( writeAttempts );	//
		
		if ( isClosed.get() ) {
			sbuffer.append(", closeTime=").append( TimeUtil.formatTimestamp( closeTime ) );
			sbuffer.append(", closeReason=").append( closeReason );
		}
		sbuffer.append(", isClosed=").append( isClosed.get() );
		
		sbuffer.append("]");
		return  sbuffer.toString();
	}
	
	public void releaseLock() {
		_readLock.set(false);
	}
	
	@Override
	public boolean isFlowLimit() {
		UserCfg uc = this.getUserCfg();
		if ( uc != null )
			return uc.isFlowLimit();
		return false;
	}
	
	@Override
	public void flowClean() {
		super.flowClean();
		this.write(RedisFrontSession.FLOW_LIMIT);
		this.close("flow limit");
	}
}
