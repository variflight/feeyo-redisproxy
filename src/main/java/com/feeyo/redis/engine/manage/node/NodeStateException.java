package com.feeyo.redis.engine.manage.node;

public class NodeStateException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public NodeStateException(String message) {
		super(message);
	}

	public NodeStateException(String message, Throwable cause) {
		super(message, cause);
	}

}
