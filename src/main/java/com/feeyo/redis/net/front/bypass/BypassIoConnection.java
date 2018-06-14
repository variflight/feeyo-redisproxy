package com.feeyo.redis.net.front.bypass;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.RedisRequest;
import com.feeyo.net.codec.RedisResponse;
import com.feeyo.net.codec.RedisResponseDecoder;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.JedisHolder;
import com.feeyo.util.jedis.JedisPool;

public class BypassIoConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BypassIoConnection.class );
	
	private String host;
	private int port;
	
	public BypassIoConnection(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	//
	public List<RedisResponse> writeToBackend(RedisRequest request) {	
		JedisPool jedisPool = JedisHolder.INSTANCE().getJedisPool(host, port);
		JedisConnection jedisConn = jedisPool.getResource();
		try {
			// block i/o
			jedisConn.sendCommand(request.getArgs());
			byte[] response = jedisConn.getBinaryReply();
			// parse
			RedisResponseDecoder decoder = new RedisResponseDecoder();
			List<RedisResponse> result = decoder.decode(response);
			while (result == null) {
				response = jedisConn.getBinaryReply();
				result = decoder.decode(response);
			}
			return result;
			
		} catch (Exception e) {
			LOGGER.error("bypass err, host=" + host + ":" + port, e);
		} finally {
			if (jedisConn != null) {
				jedisConn.close();
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

		if ( response.type() == '+' || response.type() == '-' || response.type() == ':' || response.type() == '$' ) {

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
