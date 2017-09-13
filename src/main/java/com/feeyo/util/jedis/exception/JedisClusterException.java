package com.feeyo.util.jedis.exception;

public class JedisClusterException extends JedisDataException {

	private static final long serialVersionUID = -6148546813587255525L;

	public JedisClusterException(Throwable cause) {
		super(cause);
	}

	public JedisClusterException(String message, Throwable cause) {
		super(message, cause);
	}

	public JedisClusterException(String message) {
		super(message);
	}

}
