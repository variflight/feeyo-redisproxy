package com.feeyo.net.codec;

public class RedisRequestUnknowException extends Exception {

	private static final long serialVersionUID = 7243824194940173465L;

	public RedisRequestUnknowException() {
		super();
	}
	
	public RedisRequestUnknowException(final String message) {
		super(message);
	}

	public RedisRequestUnknowException(final Throwable cause) {
		super(cause);
	}

	public RedisRequestUnknowException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
