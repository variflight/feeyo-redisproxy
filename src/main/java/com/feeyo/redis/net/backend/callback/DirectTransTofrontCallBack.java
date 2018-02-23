package com.feeyo.redis.net.backend.callback;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.codec.RedisResponse;
import com.feeyo.redis.engine.codec.RedisResponseDecoderV5;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.nio.util.TimeUtil;

/**
 * direct transfer bakend data to front connection must attach (setAttachement)
 * front connection on backend connection
 * 
 * @author zhuam
 *
 */
public class DirectTransTofrontCallBack extends AbstractBackendCallback {

	private static Logger LOGGER = LoggerFactory.getLogger( DirectTransTofrontCallBack.class );
	
	protected RedisResponseDecoderV5 decoder = new RedisResponseDecoderV5();
	
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
			
			ByteBuffer buf = (ByteBuffer)response.data() ;
			tmpSize += buf.position();
			
			frontCon.write( buf );
			
			// fast GC
			response.clear();
			
		} else {
			if  ( response.data() instanceof ByteBuffer ) {
				ByteBuffer buf = (ByteBuffer)response.data() ;
				tmpSize += buf.position();
				frontCon.write( buf );
				
				// fast GC
				response.clear();
				
			} else {
				RedisResponse[] items = (RedisResponse[]) response.data();
				for(int i = 0; i < items.length; i++) {
					if ( i == 0 ) {
						ByteBuffer buf = (ByteBuffer)items[i].data() ;
						tmpSize += buf.position();
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
	public void handleResponse(RedisBackendConnection backendCon, ByteBuffer byteBuff) throws IOException {

		// 应答解析
		List<RedisResponse> resps = decoder.decode( byteBuff );
		if ( resps != null ) {
		
			// 获取前端 connection
			// --------------------------------------------------------------
			RedisFrontConnection frontCon = getFrontCon( backendCon );
			try {
				String password = frontCon.getPassword();
				String cmd = frontCon.getSession().getRequestCmd();
				byte[] key = frontCon.getSession().getRequestKey();
				int requestSize = frontCon.getSession().getRequestSize();
				long requestTimeMills = frontCon.getSession().getRequestTimeMills();			
				long responseTimeMills = TimeUtil.currentTimeMillis();
				int responseSize = 0;
				
				for(RedisResponse resp: resps) 
					responseSize += this.writeToFront(frontCon, resp, 0);
				
				resps.clear();	// help GC
				resps = null;
				
				// 后段链接释放
				backendCon.release();	
				// 数据收集
				StatUtil.collect(password, cmd, key, requestSize, responseSize, (int)(responseTimeMills - requestTimeMills), false);
				
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
	
	@Override
	public void connectionError(Exception e, RedisBackendConnection backendCon) {
		decoder.cleanup();
		
		super.connectionError(e, backendCon);
	}

	@Override
	public void connectionClose(RedisBackendConnection backendCon, String reason) {
		decoder.cleanup();

		super.connectionClose(backendCon, reason);
	}
	
}