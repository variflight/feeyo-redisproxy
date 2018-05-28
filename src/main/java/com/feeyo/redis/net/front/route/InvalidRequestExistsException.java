package com.feeyo.redis.net.front.route;


public class InvalidRequestExistsException extends Exception {

	private static final long serialVersionUID = 1L;
	
	// 是否容错
	private boolean isfaultTolerant = true;
	
	public InvalidRequestExistsException(String message) {
		super(message);
	}
	
	public InvalidRequestExistsException(String message, boolean isfaultTolerant) {
		super(message);
		this.isfaultTolerant = isfaultTolerant;
	}

	public InvalidRequestExistsException(String message, Throwable cause) {
		super(message, cause);
	}

	public boolean isIsfaultTolerant() {
		return isfaultTolerant;
	}

	public void setIsfaultTolerant(boolean isfaultTolerant) {
		this.isfaultTolerant = isfaultTolerant;
	}
	
}
