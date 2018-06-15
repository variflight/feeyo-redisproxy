package com.feeyo.redis.net.backend.callback;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.redis.RedisResponse;
import com.feeyo.net.codec.redis.RedisResponseDecoder;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;

/**
 * direct transfer bakend data to front connection must attach (setAttachement)
 * front connection on backend connection
 * 
 * @author zhuam
 *
 */
public class DirectTransTofrontCallBack extends AbstractBackendCallback {

	private static Logger LOGGER = LoggerFactory.getLogger( DirectTransTofrontCallBack.class );
	
	protected RedisResponseDecoder decoder = new RedisResponseDecoder();
	
	// 写入到前端
	protected int writeToFront(RedisFrontConnection frontCon, RedisResponse response, int size) throws IOException {	
		
		int tmpSize = size;		
		
		if ( frontCon.isClosed() ) {
			throw new IOException("front conn is closed!"); 
		}
		
		if ( response.type() == '+' 
				|| response.type() == '-'
				|| response.type() == ':'
				|| response.type() == '$') {
			
			byte[] buf = (byte[])response.data() ;
			tmpSize += buf.length;
			
			frontCon.write( buf );
			
			// fast GC
			response.clear();
			
		} else {
			if  ( response.data() instanceof byte[] ) {
				byte[] buf = (byte[])response.data() ;
				tmpSize += buf.length;
				frontCon.write( buf );
				
				// fast GC
				response.clear();
				
			} else {
				RedisResponse[] items = (RedisResponse[]) response.data();
				for(int i = 0; i < items.length; i++) {
					if ( i == 0 ) {
						byte[] buf = (byte[])items[i].data() ;
						tmpSize += buf.length;
						frontCon.write( buf );
						
						// fast GC
						response.clear();
						
					} else {
						tmpSize = writeToFront(frontCon, items[i], tmpSize);
					}
				}
			}
		}		
		return tmpSize;
	}
	
	// 写入到前端
	protected int writeToFront(RedisFrontConnection frontCon, byte[] response, int size) throws IOException {	
		
		int tmpSize = size;

		if (frontCon.isClosed()) {
			throw new IOException("front conn is closed!");
		}

		tmpSize += response.length;
		frontCon.write(response);

		// fast GC
		response = null;

		return tmpSize;
	}
	
	@Override
	public void handleResponse(BackendConnection backendCon, byte[] byteBuff) throws IOException {

		// 应答解析
		List<RedisResponse> resps = decoder.decode( byteBuff );
		if ( resps != null ) {
		
			// 获取前端 connection
			// --------------------------------------------------------------
			RedisFrontConnection frontCon = getFrontCon( backendCon );
			try {
				String password = frontCon.getPassword();
				String cmd = frontCon.getSession().getRequestCmd();
				String key = frontCon.getSession().getRequestKey();
				int requestSize = frontCon.getSession().getRequestSize();
				long requestTimeMills = frontCon.getSession().getRequestTimeMills();			
				long responseTimeMills = TimeUtil.currentTimeMillis();
				int responseSize = 0;
				
				for(RedisResponse resp: resps) 
					responseSize += this.writeToFront(frontCon, resp, 0);
				
				resps.clear();	// help GC
				resps = null;
				
				int procTimeMills =  (int)(responseTimeMills - requestTimeMills);
				int backendWaitTimeMills = (int)(backendCon.getLastReadTime() - backendCon.getLastWriteTime());
				
				if( backendWaitTimeMills > procTimeMills ) {
					LOGGER.warn("proc time err:  requestTime={}, responseTime={}, lastReadTime={}, lastWriteTime={}",
							new Object[]{ requestTimeMills, responseTimeMills, backendCon.getLastReadTime(), backendCon.getLastWriteTime() } );
				}
				
				// 后段链接释放
				backendCon.release();	
				
				// 数据收集
				StatUtil.collect(password, cmd, key, requestSize, responseSize, procTimeMills, backendWaitTimeMills, false);
				
			} catch(IOException e2) {
				
				if ( frontCon != null) {
					frontCon.close("write err");
				}

				// 由 reactor close
				LOGGER.error("backend write to front err:", e2);
				throw e2;
			}
		}	
	}
	
}