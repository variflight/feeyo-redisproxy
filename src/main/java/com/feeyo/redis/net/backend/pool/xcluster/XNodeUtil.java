package com.feeyo.redis.net.backend.pool.xcluster;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.feeyo.net.codec.redis.RedisRequest;

public class XNodeUtil {

	// #号中间匹配若干字母和数字
	private static final Pattern pattern = Pattern.compile("\\.*#[a-zA-Z0-9]+#$");

	public static String getSuffix(RedisRequest request) {
		
		if ( request.getNumArgs() <= 1) {
			return null;
		}
		
		
		String key = new String( request.getArgs()[1] );
		Matcher m = pattern.matcher(key);
		if ( m.find() ) {
			int beginIndex = m.start();
			int endIndex = m.end();
			String suffix = key.substring(beginIndex, endIndex);
			String prefix = key.substring(0, beginIndex);
			
			// 清除后缀， 使用前缀
			request.getArgs()[1] = prefix.getBytes();
			return suffix;
		}
	
		return null;
	}
}
