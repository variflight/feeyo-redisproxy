package com.feeyo.redis.net.front.bypass;

public class BeyondTaskQueueException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public BeyondTaskQueueException() {
		super();
	}

	public BeyondTaskQueueException(final String message) {
		super(message);
	}
}
