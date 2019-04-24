package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.prefix.KeyIllegalException;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 没有任何Key 需要改变
 * 
 * @author zhuam
 *
 */
public class NoKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalException {
		// ignore
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		return null;
	}

}
