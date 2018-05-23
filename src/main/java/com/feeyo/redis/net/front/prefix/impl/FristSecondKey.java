package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 变换前两个Key
 * 
 * @author zhuam
 *
 */
public class FristSecondKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, byte[] prefix) {
		byte[][] args = request.getArgs();
		for (int i = 1; i < 3; i++) {
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
