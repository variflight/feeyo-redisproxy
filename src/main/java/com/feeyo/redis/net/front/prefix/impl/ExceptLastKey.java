package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.RedisRequest;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 除了最后一个Key, 其他Key 都需要变换
 * 
 * @author zhuam
 *
 */
public class ExceptLastKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, byte[] prefix) {
		byte[][] args = request.getArgs();
		for (int i = 1; i < args.length - 1; i++) {						
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
