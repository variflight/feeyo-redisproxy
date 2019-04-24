package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.prefix.KeyIllegalException;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 除了前两个
 * 
 * @author zhuam
 *
 */
public class ExceptFirstKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalException {
		byte[][] args = request.getArgs();
		for (int i = 2; i < args.length; i++) {
			args[i] = concat(userCfg, args[i]);
		}
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		if ( request.getNumArgs() >  2 )
			return request.getArgs()[2];
		return null;
	}

}
