package com.feeyo.redis.engine.manage.node;

public class SlotRemoveException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public SlotRemoveException(String message) {
		super(message);
	}

	public SlotRemoveException(String message, Throwable cause) {
		super(message, cause);
	}
}
