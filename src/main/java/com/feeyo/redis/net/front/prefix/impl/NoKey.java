package com.feeyo.redis.net.front.prefix.impl;

import com.feeyo.net.codec.RedisRequest;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;

/**
 * 没有任何Key 需要改变
 * 
 * @author zhuam
 *
 */
public class NoKey extends KeyPrefixStrategy {

	@Override
	public void rebuildKey(RedisRequest request, byte[] prefix) {
		// ignore
	}

	@Override
	public byte[] getKey(RedisRequest request) {
		return null;
	}

}
