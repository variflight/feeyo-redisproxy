package com.feeyo.redis.net.front.route;


public class InvalidRequestExistsException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public InvalidRequestExistsException(String message) {
		super(message);
	}

	public InvalidRequestExistsException(String message, Throwable cause) {
		super(message, cause);
	}
}
