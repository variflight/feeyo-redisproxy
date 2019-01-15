package com.feeyo.redis.net.front.prefix;

/**
 * 所有请求都不需要透传
 * 
 * @author zhuam
 *
 */
public class KeyIllegalCharacterException extends Exception {

	private static final long serialVersionUID = -7389705871040422092L;
	
	
	public KeyIllegalCharacterException(String message) {
		super(message);
	}

	public KeyIllegalCharacterException(String message, Throwable cause) {
		super(message, cause);
	}
}
