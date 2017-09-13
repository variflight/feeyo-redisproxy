package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 第二个Key 变换
 * 
 * @author zhuam
 *
 */
public class SecondKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, byte[] prefix) {
		byte[][] args = request.getArgs();
		if (args == null || args.length < 3 ) {
			return;
		}
		args[2] = concat(prefix, args[2]);		
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		byte[][] args = request.getArgs();
		if ( args == null || args.length < 3 ) {
			return null;
		}
		return args[2];
	}

}
