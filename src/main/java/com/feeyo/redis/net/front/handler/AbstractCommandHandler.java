package com.feeyo.redis.net.front.handler;

import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.BackendCallback;
import com.feeyo.redis.net.backend.callback.DelegateCallback;
import com.feeyo.redis.net.backend.callback.SelectDbCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;

import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.route.RouteResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 负责处理前端指令解析,分发
 * 
 * @see http://redis.io/topics/protocol
 * @see http://redisbook.com/preview/server/execute_command.html
 * @see https://wizardforcel.gitbooks.io/redis-doc/content/doc/5.html
 *  
 * @author zhuam
 *
 */
public abstract class AbstractCommandHandler {
	
	private static Logger LOGGER = LoggerFactory.getLogger( AbstractCommandHandler.class );
	
	public static final byte[] NOT_SUPPORTED = "-ERR Not supported.\r\n".getBytes();
	
	public static final String PIPELIEN_CMD = "pipeline";
	public static final byte[] PIPELIEN_KEY = PIPELIEN_CMD.getBytes();
	
	protected RedisFrontConnection frontCon;
	
	public AbstractCommandHandler(RedisFrontConnection frontCon) {
	        this.frontCon = frontCon;
	}
	
	public void handle(RouteResult rrs) {
		try {
			commonHandle( rrs );
		} catch (IOException e1) {
			LOGGER.error("front handle err:", e1);
			try {
				String error = "-ERR " + e1.getMessage() + ".\r\n";
				frontCon.write(error.getBytes());
			} catch (IOException e) {
			}
			return;
		}
	}
	
	protected abstract void commonHandle(RouteResult rrs) throws IOException;
	
	/*
	 * 写出数据
	 * 1、从连接池，没有闲置的需要手动新建
	 * 2、新建及闲置连接都需要考虑 非集群情况下的自动 select database 问题
	 */
	protected RedisBackendConnection writeToBackend(PhysicalNode node, final ByteBuffer buffer, 
			BackendCallback callback) throws IOException {
		
		//选择后端连接
		final int poolType = node.getPoolType();
		RedisBackendConnection backendCon = node.getConnection(callback, frontCon);
		if ( backendCon == null ) {
			
			// 新建连接，通过代理callback 处理 select database 
			DelegateCallback delegateCallback = new DelegateCallback( callback ) {
				@Override
				public void connectionAcquired(RedisBackendConnection conn) {
					
					try {
						// 集群不需要处理 select database
						if ( poolType == 1) {
							conn.write( buffer );
				
						} else {
							// 执行 SELECT 指令
							int db = frontCon.getUserCfg().getSelectDb();
							if (conn.needSelectIf(db)) {
								
								SelectDbCallback selectDbCallback = new SelectDbCallback(db, conn.getCallback(), buffer);
								conn.select(db, selectDbCallback);
				
							} else {
								conn.write( buffer );
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
			};
			backendCon = node.createNewConnection(delegateCallback, frontCon, true);
			
		} else {
			
			// 集群不需要处理 select index
			if ( poolType == 1) {
				backendCon.write( buffer );

			} else {
				// 执行 SELECT 指令
				int db = frontCon.getUserCfg().getSelectDb();
				if (backendCon.needSelectIf(db)) {
					SelectDbCallback selectDbCallback = new SelectDbCallback(db, backendCon.getCallback(), buffer);
					backendCon.select(db, selectDbCallback);

				} else {
					backendCon.write( buffer );
				}
			}
		}
		return backendCon;
	}
	
	// FRONT CONNECTION EVENT 
	// ---------------------------------------------------------------------------------------
	public void frontConnectionClose(String reason) {
		frontCon.releaseLock();
	}
	public void frontHandlerError(Exception e) {
		frontCon.releaseLock();
	}
	
	// BACKEND CONNECTION EVENT 
	// ---------------------------------------------------------------------------------------
	public void backendConnectionError(Exception e) {
		frontCon.releaseLock();
	}
	public void backendConnectionClose(String reason) {
		frontCon.releaseLock();
	}
	public void backendHandlerError(Exception e) {
		frontCon.releaseLock();
	}

}
