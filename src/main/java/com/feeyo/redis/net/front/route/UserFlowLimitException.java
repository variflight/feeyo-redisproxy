package com.feeyo.redis.net.front.route;

public class UserFlowLimitException  extends Exception {

	private static final long serialVersionUID = 1L;
	
	public UserFlowLimitException(String message) {
		super(message);
	}

	public UserFlowLimitException(String message, Throwable cause) {
		super(message, cause);
	}

}
