package com.feeyo.redis.net.front.route;

import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;

public class InvalidRequestExistsException extends Exception {

	private static final long serialVersionUID = 1L;
	
	private List<RedisRequest> requests;
	
	public InvalidRequestExistsException(String message, List<RedisRequest> requests) {
		super(message);
		this.requests = requests;
	}

	public InvalidRequestExistsException(String message, Throwable cause,  List<RedisRequest> requests) {
		super(message, cause);
		this.requests = requests;
	}
	
	public List<RedisRequest> getRequests() {
		return requests;
	}

}
