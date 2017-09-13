package com.feeyo.util.jedis.exception;

import com.feeyo.util.jedis.HostAndPort;

public class JedisAskDataException extends JedisRedirectionException {

	private static final long serialVersionUID = 142573222312730051L;

	public JedisAskDataException(Throwable cause, HostAndPort targetHost, int slot) {
		super(cause, targetHost, slot);
	}

	public JedisAskDataException(String message, Throwable cause, HostAndPort targetHost, int slot) {
		super(message, cause, targetHost, slot);
	}

	public JedisAskDataException(String message, HostAndPort targetHost, int slot) {
		super(message, targetHost, slot);
	}

}
