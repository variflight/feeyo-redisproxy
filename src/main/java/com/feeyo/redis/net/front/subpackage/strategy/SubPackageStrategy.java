package com.feeyo.redis.net.front.subpackage.strategy;

import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.ext.Segment;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;

public abstract class SubPackageStrategy {
	
	/*
	 * 初始request中按照key对应的slot所在的cluster node来分组，用以分包成多个mset，mget，del，exists
	 */
	public abstract RedisRequestType subPackage(RedisRequest request, RedisRequestPolicy requestPolicy,
			List<RedisRequest> newRequests, List<RedisRequestPolicy> newRequestPolicys, List<Segment> segments)
			throws InvalidRequestExistsException;
	
}
