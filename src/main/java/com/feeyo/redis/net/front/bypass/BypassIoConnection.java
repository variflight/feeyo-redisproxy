package com.feeyo.redis.net.front.bypass;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisResponse;
import com.feeyo.redis.net.codec.RedisResponseDecoder;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.JedisHolder;
import com.feeyo.util.jedis.JedisPool;

public class BypassIoConnection {
	private static Logger LOGGER = LoggerFactory.getLogger( BypassIoConnection.class );
	
	private String backendHost;
	private int backendPort;
	
	public BypassIoConnection(String backendHost, int backendPort) {
		this.backendHost = backendHost;
		this.backendPort = backendPort;
	}
	
	public List<RedisResponse> writeToBackend(RedisRequest request) {
		JedisPool jedisPool = JedisHolder.INSTANCE().getJedisPool(backendHost, backendPort);
		JedisConnection conn = jedisPool.getResource();
		try {
			conn.sendCommand(request.getArgs());
			
			RedisResponseDecoder decoder = new RedisResponseDecoder();
			byte[] response = conn.getBinaryReply();
			
			return decoder.decode(response);
			
		} catch (Exception e) {
			LOGGER.error("bypass err, host={},port={},exception={}", new Object[] {backendHost, backendPort, e});
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		
		return null;
	}
	
	
	// 写入到前端
	public int writeToFront(RedisFrontConnection frontCon, RedisResponse response, int size) throws IOException {

		int tmpSize = size;

		if (frontCon.isClosed()) {
			throw new IOException("front conn is closed!");
		}

		if (response.type() == '+' || response.type() == '-' || response.type() == ':' || response.type() == '$') {

			byte[] buf = (byte[]) response.data();
			tmpSize += buf.length;

			frontCon.write(buf);

			// fast GC
			response.clear();

		} else {
			if (response.data() instanceof byte[]) {
				byte[] buf = (byte[]) response.data();
				tmpSize += buf.length;
				frontCon.write(buf);

				// fast GC
				response.clear();

			} else {
				RedisResponse[] items = (RedisResponse[]) response.data();
				for (int i = 0; i < items.length; i++) {
					if (i == 0) {
						byte[] buf = (byte[]) items[i].data();
						tmpSize += buf.length;
						frontCon.write(buf);

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
}
