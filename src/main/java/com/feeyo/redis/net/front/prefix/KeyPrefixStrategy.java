package com.feeyo.redis.net.front.prefix;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.redis.config.UserCfg;

/**
 * Key 前缀策略
 * 
 * @author zhuam
 *
 */
public abstract class KeyPrefixStrategy {
	
	public static final int AllKey = 1;
	public static final int ExceptFirstKey = 2;
	public static final int ExceptLastKey = 3;
	public static final int FirstKey = 4;
	public static final int FristSecondKey = 5;
	public static final int MKey = 6;
	public static final int NoKey = 7;
	public static final int SecondKey = 8;
	
	protected byte[] concat(byte[] prefix, byte[] key) {
		if (prefix == null) {
			return key;
		}
		int length = prefix.length + key.length;
		byte[] result = new byte[length];
		
		System.arraycopy(prefix, 0, result, 0, prefix.length);
		System.arraycopy(key, 0, result, prefix.length, key.length);
		
		return result;
	}
	
	protected void illegalCharacterFilter(byte[] key, UserCfg userCfg) throws KeyIllegalCharacterException {
		String k = new String(key);
		if (userCfg.getKeyRule() != null && !userCfg.getKeyRule().matcher(k).find()) {
			throw new KeyIllegalCharacterException(k + " has illegal character");
		}
	}
	
	/**
	 * 重新构建 key
	 */
	public abstract  void rebuildKey(RedisRequest request, UserCfg userCfg) throws KeyIllegalCharacterException;
	
	/**
	 * 获取新的路由 key
	 */
	public abstract byte[] getKey(RedisRequest request);

}
