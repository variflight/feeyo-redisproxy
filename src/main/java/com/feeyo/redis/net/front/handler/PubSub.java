package com.feeyo.redis.net.front.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisResponse;
import com.feeyo.redis.net.backend.TodoTask;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.BackendCallback;
import com.feeyo.redis.net.backend.callback.DirectTransTofrontCallBack;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.nio.NetSystem;

/**
 * 用于支持 PubSub
 * 
 * @author zhuam
 */
public class PubSub  {
	
	private static Logger LOGGER = LoggerFactory.getLogger( PubSub.class );
	
	private RedisFrontConnection frontCon = null;
	private RedisBackendConnection backendCon = null;
	private PhysicalNode node = null;
	
	private BackendCallback unsubscribeAllCallback;
	
	private final AtomicBoolean isClosed;
	
	/**
	 *  subscribe a b c d ... 
	 */
	private List<String> channelNames = new ArrayList<String>();
	
	public PubSub(RedisFrontConnection frontCon, BackendCallback unsubscribeAllCallback) {
		
		
		this.frontCon = frontCon;
		this.unsubscribeAllCallback = unsubscribeAllCallback;
		this.isClosed = new AtomicBoolean(false);
	}
	
	public boolean isClosed() {
		return isClosed.get();
	}
	
	// 写入至后端
	private void writeToBackend(final RedisRequest request) throws IOException {

		if ( backendCon == null ) {

			DirectTransTofrontCallBack callback = new DirectTransTofrontCallBack() {
				@Override
				public void handleResponse(RedisBackendConnection backendCon, byte[] byteBuff) throws IOException {
					// 应答解析
					List<RedisResponse> resps = decoder.decode(byteBuff);
					if (resps != null) {
						// 写入至前端
						RedisFrontConnection frontCon = getFrontCon(backendCon);
						for (RedisResponse resp : resps)
							this.writeToFront(frontCon, resp, 0);

						resps.clear();
						resps = null;
					}
				}
			};
			
			// 连接建立成功后需要处理的任务
			TodoTask task = new TodoTask() {				
				@Override
				public void execute(RedisBackendConnection backendCon) throws Exception {	
					backendCon.write(  request.encode(), false  );
				}
			};
			callback.addTodoTask(task);
			
			// 创建新连接
			this.backendCon = node.createNewConnection(callback, frontCon);
			
		} else {
			
			backendCon.write(request.encode(), false);
		}
	}
	
	public void subscribe(RedisRequest request, PhysicalNode node) throws IOException {
		this.node = node;

		for (int i = 1; i < request.getNumArgs(); i++) {
			channelNames.add(new String(request.getArgs()[i]));
		}

		// 写入至后端
		writeToBackend(request);

		// 设置超时时间
		frontCon.setIdleTimeout(1 * 60 * 60 * 1000L);
		backendCon.setIdleTimeout(1 * 60 * 60 * 1000L);
	}

	public void unsubscribe(RedisRequest request) throws IOException {

		for(int i = 1; i < request.getNumArgs(); i++) {
			String channelName = new String( request.getArgs()[i] );
			if ( channelName.equals( "*" ) ) {
				channelNames.clear();
				break;
			}
			channelNames.remove( channelName );
		}
		
		if ( channelNames.isEmpty()   ) {
			// 取消所有订阅
			unsubscribeAll();
			
		} else {
			// 取消部分订阅
			writeToBackend(request);
		}
	}

	// 取消所有的订阅
	public void unsubscribeAll() {
		
		LOGGER.warn("##pubsub unsubscribeAll");
		
		isClosed.set(true);
		
		if ( backendCon != null ) {
			try {
				backendCon.setIdleTimeout( NetSystem.getInstance().getNetConfig().getBackendIdleTimeout() );				
				backendCon.unsubscribe( unsubscribeAllCallback );
			} catch (IOException e) {
				LOGGER.error("##pubsub cleanup err:", e);
				backendCon.close( e.toString() );
			}
		}
		
		// 做一些重置
		if ( frontCon != null ) {
			frontCon.setIdleTimeout( NetSystem.getInstance().getNetConfig().getFrontIdleTimeout()  );
		}
	}
	
	public void close() {
		
		isClosed.set(true);
		
		if ( frontCon != null ) {
			frontCon = null;
		}
		
		if ( backendCon != null ) {
			backendCon.release();
			backendCon = null;
		}
		
		if ( channelNames != null ) {
			channelNames.clear();
			channelNames = null;
		}
		
		unsubscribeAllCallback = null;
	}

}