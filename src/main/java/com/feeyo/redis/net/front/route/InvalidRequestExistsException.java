package com.feeyo.redis.net.front.route;

import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;

public class InvalidRequestExistsException extends Exception {

	private static final long serialVersionUID = 1L;
	
	private List<RedisRequest> requests;
	private List<RedisRequestPolicy> requestPolicys;
	
	public InvalidRequestExistsException(String message, List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys) {
		super(message);
		this.requests = requests;
		this.requestPolicys = requestPolicys;
	}

	public InvalidRequestExistsException(String message, Throwable cause,  List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys) {
		super(message, cause);
		this.requests = requests;
		this.requestPolicys = requestPolicys;
	}
	
	public List<RedisRequest> getRequests() {
		return requests;
	}

	public List<RedisRequestPolicy> getRequestPolicys() {
		return requestPolicys;
	}

}
