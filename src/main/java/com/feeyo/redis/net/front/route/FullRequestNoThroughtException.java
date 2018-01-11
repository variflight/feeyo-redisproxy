package com.feeyo.redis.net.front.route;

import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;

/**
 * 自动响应
 * 
 * @author zhuam
 *
 */
public class FullRequestNoThroughtException extends Exception {

	private static final long serialVersionUID = -7389705871040422092L;
	
	private List<RedisRequest> requests;
	
	public FullRequestNoThroughtException(String message, List<RedisRequest> requests) {
		super(message);
		this.requests = requests;
	}

	public FullRequestNoThroughtException(String message, Throwable cause, List<RedisRequest> requests) {
		super(message, cause);
		this.requests = requests;
	}

	public List<RedisRequest> getRequests() {
		return requests;
	}
}
