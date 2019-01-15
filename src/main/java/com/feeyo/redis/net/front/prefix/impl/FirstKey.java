package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.prefix.KeyIllegalCharacterException;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 变换第一个Key
 * @author zhuam
 *
 */
public class FirstKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalCharacterException {
		if ( request.getNumArgs() < 2) {
			return;
		}		
		byte[][] args = request.getArgs();
		illegalCharacterFilter(args[1], userCfg);
		args[1] = concat(userCfg.getPrefix(), args[1]);		
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
