package com.feeyo.util.jedis;

import java.util.List;

import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisResponse;
import com.feeyo.redis.net.codec.RedisResponseDecoder;

public class JedisClient extends JedisConnection{

	public JedisClient(String host, int port) {
		super(host, port);
	}
	
	
	public void sendCommand(List<RedisRequest> requests) {
		
		if(requests == null || requests.size() == 0)
			return;
		
		for(RedisRequest request :requests) {
			sendCommand(request);
		}
	}
	
	public void sendCommand(RedisRequest request) {
			
		if (request == null || request.getArgs().length == 0) {
			return;
		}
		
		byte[] cmd = request.getArgs()[0];
		byte[][] args = new byte[request.getNumArgs() -1][];
		System.arraycopy(request.getArgs(), 1, args, 0, request.getNumArgs() -1);
		sendCommand(RedisCommand.valueOf(new String(cmd).toUpperCase()),args);
	}
	
	public List<RedisResponse> getResponses() {
		RedisResponseDecoder decoder = new RedisResponseDecoder();
		byte[] response = getBinaryReply();
		
		return decoder.decode(response);
	}
	
	public static void main(String[] args) {
		System.out.println(new String(RedisCommand.AUTH.getRaw()));
		
	}
	
}