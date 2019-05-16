package com.feeyo.redis.net.front.rewrite.impl;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.rewrite.KeyIllegalException;
import com.feeyo.redis.net.front.rewrite.KeyRewriteStrategy;

/**
 * EVAL 定义
 * EVAL script numkeys key [key ...] arg [arg ...]
 * 
 * TODO：About redis cluster
 * 在一个节点接收到 EVAL 指令之后，他会检查 KEYS，算出对应的 Slots，
 * 如果所有 KEY 不是落到同一个 Slot 上，会提示 CROSSSLOT Keys in request don't hash to the same slot
 * 
 *
 */
public class EvalKey extends KeyRewriteStrategy {
	
	/**
	 * fix
	 * cmd = HGET
	 * params= [
		arg=EVAL
		arg=local hexists = redis.call("hexists", KEYS[1], KEYS[2])
		if hexists == 0 then
		    redis.call("hset", KEYS[1], KEYS[2], ARGV[1])
		end
		return redis.call("hget", KEYS[1], KEYS[2])
		
		arg=2
		arg=bav:1562198400
		arg=HFE:WXN:GY7124
		arg={"id":288485281214173185,"md5":"CBC1C8040A589C3014E4986828CE0D3D","saved":false,"updateTime":1557891856456}
		]
	 */

	@Override
	public void rewriteKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalException {
		byte[][] args = request.getArgs();
		if (args.length < 4) {
			throw new KeyIllegalException("EVAL command must put keys to params, if this is no params, please do not use 'EVAL' !");
		}
		
		int keySize = Integer.parseInt(new String(args[2]));
		for (int i = 3; i < keySize + 3; i++) {
			checkKeyIllegalCharacter(userCfg.getKeyRegularExpr(), args[i]);
			//
			args[i] = concat(userCfg.getPrefix(), args[i]);
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
