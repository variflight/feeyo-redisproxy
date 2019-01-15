package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.prefix.KeyIllegalCharacterException;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 
 * 所有的KEY 进行变换
 * 
 * @author zhuam
 *
 */
public class AllKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalCharacterException {
		byte[][] args = request.getArgs();
		for (int i = 1; i < args.length; i++) {	
			illegalCharacterFilter(args[i], userCfg);
			args[i] = concat(userCfg.getPrefix(), args[i]);
		}
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		if ( request.getNumArgs() >  1 )
			return request.getArgs()[1];
		return null;
	}
}
