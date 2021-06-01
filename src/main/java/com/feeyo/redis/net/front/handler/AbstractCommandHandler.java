package com.feeyo.redis.net.front.handler;

import com.feeyo.redis.net.backend.TodoTask;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.AbstractBackendCallback;
import com.feeyo.redis.net.backend.callback.SelectDbCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.backend.pool.PoolType;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.route.RouteResult;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 负责处理前端指令解析,分发
 * 
 * @see http://redis.io/topics/protocol
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
			
			String reason = ExceptionUtils.getStackTrace(e1);
			LOGGER.error("front handle err: {}", reason);
			//frontCon.writeErrMessage( e1.getMessage() );
			//
			frontCon.close( e1.getMessage() );
			return;
		}
	}
	
	protected abstract void commonHandle(RouteResult rrs) throws IOException;
	
	/*
	 * 写出数据
	 * 1、从连接池，没有闲置的需要手动新建
	 * 2、新建及闲置连接都需要考虑 非集群情况下的自动 select database 问题
	 */
	protected BackendConnection writeToBackend(PhysicalNode node, final ByteBuffer buffer, 
			AbstractBackendCallback callback) throws IOException {
		
		//选择后端连接
		final int poolType = node.getPoolType();
		BackendConnection backendCon = node.getConnection(callback, frontCon);
		if ( backendCon == null ) {
			
			TodoTask task = new TodoTask() {				
				@Override
				public void execute(BackendConnection conn) throws Exception {	
					
					switch (poolType) {
					case PoolType.REDIS_STANDALONE: // 非集群
						{ 
							// 执行 SELECT 指令
							RedisBackendConnection bkCon = (RedisBackendConnection)conn;
							int db = frontCon.getUserCfg().getSelectDb();
							if (bkCon.needSelectIf(db)) {
								SelectDbCallback selectDbCallback = new SelectDbCallback(db, bkCon.getCallback(), buffer);
								bkCon.select(db, selectDbCallback);
				
							} else {
								bkCon.write( buffer );
							}
						}
						break;
					case PoolType.REDIS_CLUSTER:	// 集群
					case PoolType.REDIS_X_CLUSTER:	// 自定义集群
					case PoolType.KAFKA_CLUSTER:	// KAFKA 集群
						conn.write(buffer);
						break;

					}
				}
			};
			callback.addTodoTask(task);
			backendCon = node.createNewConnection(callback, frontCon);
			
		} else {
			
			switch (poolType) {
			case PoolType.REDIS_STANDALONE: // 非集群
				{
					// 执行 SELECT 指令
					RedisBackendConnection bkCon = (RedisBackendConnection)backendCon;
					int db = frontCon.getUserCfg().getSelectDb();
					if (bkCon.needSelectIf(db)) {
						SelectDbCallback selectDbCallback = new SelectDbCallback(db, bkCon.getCallback(), buffer);
						bkCon.select(db, selectDbCallback);
	
					} else {
						bkCon.write( buffer );
					}
				}
				break;
			case PoolType.REDIS_CLUSTER:	// 集群
			case PoolType.REDIS_X_CLUSTER:	// 自定义集群
			case PoolType.KAFKA_CLUSTER: 	// KAFKA 集群
				backendCon.write( buffer );
				break;
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

}