package com.feeyo.redis.net.front.route.strategy.segment;

import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.segment.Segment;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;

public abstract class SegmentStrategy {
	
	/*
	 * 初始request中按照key对应的slot所在的cluster node来分组，用以分包成多个mset，mget，del，exists
	 */
	public abstract RedisRequestType unpack(RedisRequest request, List<RedisRequest> newRequests, List<Segment> segments)
			throws InvalidRequestExistsException;
	
}
