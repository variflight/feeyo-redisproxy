package com.feeyo.util.jedis.exception;

public class JedisNoScriptException extends JedisDataException {

	private static final long serialVersionUID = -5369333901812450826L;

	public JedisNoScriptException(String message) {
		super(message);
	}

	public JedisNoScriptException(final Throwable cause) {
		super(cause);
	}

	public JedisNoScriptException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
