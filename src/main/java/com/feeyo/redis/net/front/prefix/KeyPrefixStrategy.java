package com.feeyo.redis.net.front.prefix;

import com.feeyo.net.codec.redis.RedisRequest;

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
	
	protected byte[] concat(byte[]... arrays) {
		int length = 0;
		for (byte[] array : arrays) {
			length += array.length;
		}
		byte[] result = new byte[length];
		int pos = 0;
		for (byte[] array : arrays) {
			System.arraycopy(array, 0, result, pos, array.length);
			pos += array.length;
		}
		return result;
	}
	
	/**
	 * 重新构建 key
	 */
	public abstract  void rebuildKey(RedisRequest request, byte[] prefix);
	
	/**
	 * 获取新的路由 key
	 */
	public abstract byte[] getKey(RedisRequest request);

}
