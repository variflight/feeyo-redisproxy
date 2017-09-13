package com.feeyo.util.jedis.exception;

public class JedisBusyException extends JedisDataException {

	private static final long serialVersionUID = -4067922568357815376L;

	public JedisBusyException(final String message) {
		super(message);
	}

	public JedisBusyException(final Throwable cause) {
		super(cause);
	}

	public JedisBusyException(final String message, final Throwable cause) {
		super(message, cause);
	}

}
