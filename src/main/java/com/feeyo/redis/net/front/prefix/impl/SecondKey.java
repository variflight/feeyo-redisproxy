package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.prefix.KeyIllegalException;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 第二个Key 变换
 * 
 * @author zhuam
 *
 */
public class SecondKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalException {
		byte[][] args = request.getArgs();
		if (args == null || args.length < 3 ) {
			return;
		}
		//
		checkIllegalCharacter(userCfg.getKeyRule(), args[2]);
		//
		args[2] = concat(userCfg.getPrefix(), args[2]);		
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
