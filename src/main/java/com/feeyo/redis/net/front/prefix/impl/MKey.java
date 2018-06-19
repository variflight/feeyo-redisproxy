package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 步长 2, 变换 key
 * 
 * @author zhuam
 *
 */
public class MKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, byte[] prefix) {
		byte[][] args = request.getArgs();
		for (int i = 1; i < args.length; i = i + 2) {
			args[i] = concat(prefix, args[i]);
		}
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		if ( request.getNumArgs() >  1 )
			return request.getArgs()[1];
		return null;
	}
}
