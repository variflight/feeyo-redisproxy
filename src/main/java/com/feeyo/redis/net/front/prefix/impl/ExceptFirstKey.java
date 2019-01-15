package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.prefix.KeyIllegalCharacterException;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 除了前两个
 * 
 * @author zhuam
 *
 */
public class ExceptFirstKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalCharacterException {
		byte[][] args = request.getArgs();
		for (int i = 2; i < args.length; i++) {
			illegalCharacterFilter(args[i], userCfg);
			args[i] = concat(userCfg.getPrefix(), args[i]);
		}
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		if ( request.getNumArgs() >  2 )
			return request.getArgs()[2];
		return null;
	}

}
