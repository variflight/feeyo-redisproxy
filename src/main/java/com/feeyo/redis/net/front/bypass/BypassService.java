package com.feeyo.redis.net.front.bypass;

import java.util.List;

import com.feeyo.redis.net.codec.RedisRequest;

// 旁路服务
public class BypassService {

	// 单例
	// 固定 coreSize maxSize queue threadPool
	// front to backend 同步请求
	// 支持  bigkey size 设置的能力、 reload 能力
	// 支持 request & response bigkey 检测 ， response bigkey, 需要 ttl scan 
	// 第一步支持 单请求， 后续考虑支持 pipeline 等 
	// 
	public void goQueuing(List<RedisRequest>  requests) {
		
		//
	}
	
}
