package com.feeyo.net.codec;

public class UnknowProtocolException extends Exception {

	private static final long serialVersionUID = 7243824194940173465L;

	public UnknowProtocolException() {
		super();
	}
	
	public UnknowProtocolException(final String message) {
		super(message);
	}

	public UnknowProtocolException(final Throwable cause) {
		super(cause);
	}

	public UnknowProtocolException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
