package com.feeyo.util.jedis.exception;

public class JedisConnectionException extends JedisException {

	private static final long serialVersionUID = -8966529417935434138L;

	public JedisConnectionException(String message) {
		super(message);
	}

	public JedisConnectionException(Throwable e) {
		super(e);
	}

	public JedisConnectionException(String message, Throwable cause) {
		super(message, cause);
	}
}