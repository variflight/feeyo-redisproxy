package com.feeyo.util.jedis.exception;

public class JedisDataException extends JedisException {

	private static final long serialVersionUID = 6808186129580046399L;

	public JedisDataException(String message) {
		super(message);
	}

	public JedisDataException(Throwable cause) {
		super(cause);
	}

	public JedisDataException(String message, Throwable cause) {
		super(message, cause);
	}

}
