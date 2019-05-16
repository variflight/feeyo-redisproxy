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
	protected byte[] concat(byte[] prefix, byte[] key)  {
		//
		if (prefix == null) {
			return key;
		}
		
		// TODO: 此处不需要考虑{} 问题，直接前面加 prefix 即可
		//
		// RedisCluster 为了兼容 multi-key 操作，提供了“hash tags”操作，每个key可以包含自定义的“tags”，
		// 在存储的时候根据tags计算此key应该映射到哪个node上。通过“hash tags”可以强制某些keys被保存到同一个节点上，便于进行“multi key”操作。
		// 基本上如果关键字包含“{...}”，那么在{和}之间的字符串被hash，然而可能有多个匹配的{或}该算法由以下规则规定：
		// 如果key包含｛，在｛的右边有一个｝，并在第一次出现｛与第一次出现｝之间有一个或者多个字符串，那么就作为key进行hash。
		// 例如，{user1000}.following和{user1000}.followed就在同一个hash slot；foo{}{bar}整个字符被hash，foo{{bar}},{bar被hash；foo{bar}{zap},bar被hash
		
		int length = prefix.length + key.length;
		byte[] result = new byte[length];
		
		System.arraycopy(prefix, 0, result, 0, prefix.length);
		System.arraycopy(key, 0, result, prefix.length, key.length);
		return result;
		
	}
	
	/**
	 * 无效字符集检测，发现后抛出异常
	 */
	protected void checkKeyIllegalCharacter(Pattern regularExpr, byte[] key) 
			throws KeyIllegalException {
		
		if ( regularExpr == null )
			return;
		//
		String k = new String(key);
		if ( !regularExpr.matcher(k).find()) {
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
