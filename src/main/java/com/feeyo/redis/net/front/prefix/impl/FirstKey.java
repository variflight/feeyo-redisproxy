package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 变换第一个Key
 * @author zhuam
 *
 */
public class FirstKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, byte[] prefix) {
		if ( request.getNumArgs() < 2) {
			return;
		}		
		byte[][] args = request.getArgs();
		args[1] = concat(prefix, args[1]);		
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		if ( request.getNumArgs() < 2) {
			return null;
		}
		byte[][] args = request.getArgs();
		return args[1];
	}

}
