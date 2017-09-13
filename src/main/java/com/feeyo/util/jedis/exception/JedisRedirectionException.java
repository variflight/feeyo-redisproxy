package com.feeyo.util.jedis.exception;

import com.feeyo.util.jedis.HostAndPort;

public class JedisRedirectionException extends JedisDataException {

	private static final long serialVersionUID = -4686458586215630852L;
	
	private HostAndPort targetNode;
	private int slot;

	public JedisRedirectionException(String message, HostAndPort targetNode, int slot) {
		super(message);
		this.targetNode = targetNode;
		this.slot = slot;
	}

	public JedisRedirectionException(Throwable cause, HostAndPort targetNode, int slot) {
		super(cause);
		this.targetNode = targetNode;
		this.slot = slot;
	}

	public JedisRedirectionException(String message, Throwable cause, HostAndPort targetNode, int slot) {
		super(message, cause);
		this.targetNode = targetNode;
		this.slot = slot;
	}

	public HostAndPort getTargetNode() {
		return targetNode;
	}

	public int getSlot() {
		return slot;
	}
}