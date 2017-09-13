package com.feeyo.util.jedis.exception;

public class JedisException extends RuntimeException {

	private static final long serialVersionUID = -9002149336762190370L;

	public JedisException(String message) {
		super(message);
	}

	public JedisException(Throwable e) {
		super(e);
	}

	public JedisException(String message, Throwable cause) {
		super(message, cause);
	}
}
