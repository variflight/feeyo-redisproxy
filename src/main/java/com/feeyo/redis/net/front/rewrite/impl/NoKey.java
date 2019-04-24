package com.feeyo.redis.net.front.rewrite.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.rewrite.KeyIllegalException;
import com.feeyo.redis.net.front.rewrite.KeyRewriteStrategy;

/**
 * 没有任何Key 需要改变
 * 
 * @author zhuam
 *
 */
public class NoKey extends KeyRewriteStrategy {

	@Override
	public void rebuildKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalException {
		// ignore
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		return null;
	}

}
