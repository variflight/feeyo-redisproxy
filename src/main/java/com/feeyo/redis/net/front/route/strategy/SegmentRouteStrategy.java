package com.feeyo.redis.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.handler.segment.Segment;
import com.feeyo.redis.net.front.handler.segment.SegmentType;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.net.front.route.strategy.segment.SegmentStrategy;
import com.feeyo.redis.net.front.route.strategy.segment.SegmentStrategyFactory;

/**
 * pipeline && mget and mset and del and exists and default command route
 * strategy
 *
 * @author xuwenfeng
 */
public class SegmentRouteStrategy extends AbstractRouteStrategy {

	private RedisRequestType rewrite(RedisRequest request, RedisRequestPolicy requestPolicy,
			List<RedisRequest> newRequests, List<RedisRequestPolicy> newRequestPolicys, List<Segment> segments)
			throws InvalidRequestExistsException {

		byte[][] args = request.getArgs();
		String cmd = new String(args[0]).toUpperCase();
		
		SegmentStrategy strategy = SegmentStrategyFactory.getStrategy(cmd);
		return strategy.unpack( request, requestPolicy, newRequests, newRequestPolicys, segments);
	}

	@Override
	public RouteResult route(int poolId, List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys)
			throws InvalidRequestExistsException, PhysicalNodeUnavailableException {

		List<Segment> segments = new ArrayList<Segment>();

		ArrayList<RedisRequest> newRequests = new ArrayList<RedisRequest>();
		ArrayList<RedisRequestPolicy> newRequestPolicys = new ArrayList<RedisRequestPolicy>();

		RedisRequestType requestType = null;
		for (int i = 0; i < requests.size(); i++) {
			RedisRequest request = requests.get(i);
			RedisRequestPolicy requestPolicy = requestPolicys.get(i);
			
			// 
			if ( CommandParse.MGETSET_CMD == requestPolicy.getLevel()
					|| (CommandParse.DEL_CMD == requestPolicy.getLevel()  && request.getArgs().length > 2 )
					|| (CommandParse.EXISTS_CMD == requestPolicy.getLevel() && request.getArgs().length > 2) ) {
				
				requestType = rewrite(request, requestPolicy, newRequests, newRequestPolicys, segments);

			} else {
				newRequests.add(request);
				newRequestPolicys.add(requestPolicy);
				int[] indexs = { newRequests.size() - 1 };

				Segment segment = new Segment(SegmentType.DEFAULT, indexs);
				segments.add(segment);
			}
		}

		List<RouteResultNode> nodes = doSharding(poolId, newRequests, newRequestPolicys);
		requestType = requests.size() > 1 ? RedisRequestType.PIPELINE : requestType;

		RouteResult result = new RouteResult(requestType, newRequests, newRequestPolicys, nodes);
		result.setSegments(segments);
		return result;
	}
}
