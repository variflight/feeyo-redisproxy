package com.feeyo.redis.net.front.rewrite;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;

import java.util.regex.Pattern;

/**
 * Key 前缀策略
 * 
 * @author zhuam
 *
 */
public abstract class KeyRewriteStrategy {
	
	public static final int AllKey = 1;
	public static final int ExceptFirstKey = 2;
	public static final int ExceptLastKey = 3;
	public static final int FirstKey = 4;
	public static final int FristSecondKey = 5;
	public static final int MKey = 6;
	public static final int NoKey = 7;
	public static final int SecondKey = 8;
	public static final int EvalKey = 9;
    public static final int SetKey = 10;
	
	/**
	 * 增加 key 前缀
	 */
	protected byte[] concat(byte[] prefix, byte[] key) throws KeyIllegalException {
		//
		if (prefix == null) {
			return key;
		}
		int length = prefix.length + key.length;
		byte[] result = new byte[length];
		
		System.arraycopy(prefix, 0, result, 0, prefix.length);
		System.arraycopy(key, 0, result, prefix.length, key.length);
		return result;
	}
	
	/**
	 * 无效字符集检测，发现后抛出异常
	 */
	protected void checkIllegalCharacter(Pattern keyRule, byte[] key) 
			throws KeyIllegalException {
		//
		String k = new String(key);
		if ( keyRule != null && !keyRule.matcher(k).find()) {
			throw new KeyIllegalException(k + " has illegal character");
		}
	}
	
	/**
	 * 改写 key
	 */
	public abstract void rewriteKey(RedisRequest request, UserCfg userCfg) 
			throws KeyIllegalException;
	
	/**
	 * 获取新的路由 key
	 */
	public abstract byte[] getKey(RedisRequest request);

}
