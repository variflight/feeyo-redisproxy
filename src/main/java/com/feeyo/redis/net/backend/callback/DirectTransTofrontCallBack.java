package com.feeyo.redis.net.backend.callback;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.codec.RedisResponseDecoderV4;
import com.feeyo.redis.engine.codec.RedisResponseV3;
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
	
	protected RedisResponseDecoderV4 decoder = new RedisResponseDecoderV4();
	
	private static Logger LOGGER = LoggerFactory.getLogger( DirectTransTofrontCallBack.class );
	
	// 写入到前端
	protected int writeToFront(RedisFrontConnection frontCon, RedisResponseV3 response, int size) throws IOException {	
		
		int tmpSize = size;		
		
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
				RedisResponseV3[] items = (RedisResponseV3[]) response.data();
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
	
	@Override
	public void handleResponse(RedisBackendConnection backendCon, byte[] byteBuff) throws IOException {

		// 应答解析
		List<RedisResponseV3> resps = decoder.decode( byteBuff );
		if ( resps != null ) {
		
			// 获取前端 connection
			// --------------------------------------------------------------
			
			RedisFrontConnection frontCon = null;
			try {
				
				frontCon = getFrontCon( backendCon );
				String password = frontCon.getPassword();
				String cmd = frontCon.getSession().getRequestCmd();
				byte[] key = frontCon.getSession().getRequestKey();
				int requestSize = frontCon.getSession().getRequestSize();
				long requestTimeMills = frontCon.getSession().getRequestTimeMills();			
				long responseTimeMills = TimeUtil.currentTimeMillis();
				int responseSize = 0;
				
				for(RedisResponseV3 resp: resps) 
					responseSize += this.writeToFront(frontCon, resp, 0);
				
				resps.clear();	// help GC
				resps = null;
				
				// 后段链接释放
				backendCon.release();	
				backendCon.markBrokenRespAsConusmed();
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
	
}