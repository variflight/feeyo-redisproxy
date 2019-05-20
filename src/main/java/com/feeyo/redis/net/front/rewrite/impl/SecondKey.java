package com.feeyo.redis.net.front.rewrite.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.rewrite.KeyIllegalException;
import com.feeyo.redis.net.front.rewrite.KeyRewriteStrategy;

/**
 * 第二个Key 变换
 * 
 * @author zhuam
 *
 */
public class SecondKey extends KeyRewriteStrategy {

	@Override
	public void rewriteKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalException {
		byte[][] args = request.getArgs();
		if (args == null || args.length < 3 ) {
			return;
		}
		//
		checkKeyIllegalCharacter(userCfg.getKeyExpr(), args[2]);
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
