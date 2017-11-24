package com.feeyo.redis.net.front.route;

import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;

/**
 * 管理指令，自动回复
 * 
 * @author zhuam
 *
 */
public class ManageRespNotTransException extends Exception {
	
	private static final long serialVersionUID = -4827673049382354888L;
	
	private List<RedisRequest> requests;
	
	public ManageRespNotTransException(String message, 
			List<RedisRequest> requests) {
		super(message);
		this.requests = requests;
	}

	public ManageRespNotTransException(String message, Throwable cause, 
			List<RedisRequest> requests) {
		super(message, cause);
		this.requests = requests;
	}

	public List<RedisRequest> getRequests() {
		return requests;
	}

}