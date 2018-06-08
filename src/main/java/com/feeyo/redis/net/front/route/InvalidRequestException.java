package com.feeyo.redis.net.front.route;


public class InvalidRequestException extends Exception {

	private static final long serialVersionUID = 1L;
	
	// 是否容错
	private boolean isfaultTolerant = true;
	
	public InvalidRequestException(String message) {
		super(message);
	}
	
	public InvalidRequestException(String message, boolean isfaultTolerant) {
		super(message);
		this.isfaultTolerant = isfaultTolerant;
	}

	public InvalidRequestException(String message, Throwable cause) {
		super(message, cause);
	}

	public boolean isIsfaultTolerant() {
		return isfaultTolerant;
	}

	public void setIsfaultTolerant(boolean isfaultTolerant) {
		this.isfaultTolerant = isfaultTolerant;
	}
	
}
