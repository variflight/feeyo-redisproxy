package com.feeyo.redis.net.front.route;

public class PhysicalNodeUnavailableException  extends Exception {

	private static final long serialVersionUID = 1L;
	
	public PhysicalNodeUnavailableException(String message) {
		super(message);
	}

	public PhysicalNodeUnavailableException(String message, Throwable cause) {
		super(message, cause);
	}

}
