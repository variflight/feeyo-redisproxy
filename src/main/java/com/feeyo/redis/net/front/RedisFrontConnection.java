package com.feeyo.redis.net.front;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
	
	// 用户连接数
	private static Map<String, AtomicInteger> CONN_NUM = new ConcurrentHashMap<String, AtomicInteger>();
	
	// 用户配置
	private UserCfg userCfg;
	
	private String password;
	private boolean isAuthenticated;
	
	private RedisFrontSession session;

	//
	protected NetFlowGuard netflowGuard;
	
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
		//
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
	
	public String getPassword() {
		return password;
	}

	public void setPassword(String newPassword) {
		
		// 最大连接数检查
		//
		AtomicInteger num1 = CONN_NUM.get( newPassword );
		if( num1 != null ) {
			//
			if (  num1.get() > this.userCfg.getMaxCon() ) {
				//
				this.write( "-ERR Too many connections, please try again later. \r\n".getBytes() );
				this.close("maxConn limit");
				return;
			}
			
			num1.incrementAndGet();
			
		} else {
			num1 = new AtomicInteger(1);
			CONN_NUM.put(newPassword, num1);
		}
		
		
		// 去重
		//
		if ( isAuthenticated ) {
			AtomicInteger num2 = CONN_NUM.get( password );
			if ( num2 != null && !this.password.equals( newPassword ) ) {
				num2.decrementAndGet();
			}
		}
		
		//
		this.password = newPassword;
		this.isAuthenticated = true;
	}
	
	public boolean isAuthenticated() {
		return isAuthenticated;
	}
	
	public UserCfg getUserCfg() {
		return userCfg;
	}

	public void setUserCfg(UserCfg userCfg) {
		this.userCfg = userCfg;
	}
	
	public void setNetFlowGuard(NetFlowGuard netflowGuard) {
		this.netflowGuard = netflowGuard;
	}
	
	public NetFlowGuard getNetFlowGuard() {
		return netflowGuard;
	}
	

	@Override
	public void close(String reason) {
		
		super.close(reason);
		this.releaseLock();
		
		// 已认证
		if ( isAuthenticated ) {
			
			AtomicInteger num = CONN_NUM.get( password );
			if ( num != null ) {
				int v = num.decrementAndGet();
				if ( v < 0 ) {
					LOGGER.warn("CONN_NUM err, pwd:{} v:{}", password, v );
				}
			}
		}
		
	}
	
	public void releaseLock() {
		_readLock.set(false);
	}

	// 流量
	@Override
	protected boolean flowGuard(long length) {
		
		if ( netflowGuard != null && netflowGuard.consumeBytes(this.getPassword(), length) ) {
			
			LOGGER.warn("##flow clean##, front: {} ", this);
			
			//
			this.write( "-ERR netflow problem, request clean. \r\n".getBytes() );
			this.close("flow limit");
			return true;
		}
		
		return false;
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
		sbuffer.append(", startup=").append( startupTime );
		sbuffer.append(", lastRT=").append(  lastReadTime );
		sbuffer.append(", lastWT=").append( lastWriteTime );
		sbuffer.append(", attempts=").append( writeAttempts );	
		sbuffer.append(", isClosed=").append( isClosed.get() );

		sbuffer.append("]");
		return  sbuffer.toString();
	}
	
}