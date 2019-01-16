package com.feeyo.redis.net.front.prefix;

/**
 * 所有请求都不需要透传
 * 
 * @author zhuam
 *
 */
public class KeyIllegalException extends Exception {

	private static final long serialVersionUID = -7389705871040422092L;
	
	
	public KeyIllegalException(String message) {
		super(message);
	}

	public KeyIllegalException(String message, Throwable cause) {
		super(message, cause);
	}
}
