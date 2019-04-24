package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.prefix.KeyIllegalException;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

public class EvalKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalException {
		byte[][] args = request.getArgs();
		if (args.length < 4) {
			throw new KeyIllegalException(
					"eval cmd must put keys to params! if this is no params, "
					+ "please do not use eval in redis, because u can do it in your code instead!");
		}
		
		int keyLength = Integer.parseInt(new String(args[2]));
		
		for (int i = 3; i < keyLength + 3; i++) {
			//
			illegalCharacterFilter(userCfg, args[i]);
			//
			args[i] = concat(userCfg, args[i]);
		}
		
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		if (request.getArgs().length > 3) {
			return request.getArgs()[3];
		}
		return null;
	}

}
