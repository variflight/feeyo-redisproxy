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

	private RedisRequestType rewrite(RedisRequest request, List<RedisRequest> newRequests, List<Segment> segments)
			throws InvalidRequestExistsException {

		byte[][] args = request.getArgs();
		String cmd = new String(args[0]).toUpperCase();
		
		SegmentStrategy strategy = SegmentStrategyFactory.getStrategy(cmd);
		return strategy.unpack( request, newRequests, segments);
	}

	@Override
	public RouteResult route(int poolId, List<RedisRequest> requests)
			throws InvalidRequestExistsException, PhysicalNodeUnavailableException {

		List<Segment> segments = new ArrayList<Segment>();

		ArrayList<RedisRequest> newRequests = new ArrayList<RedisRequest>();

		RedisRequestType requestType = null;
		for (int i = 0; i < requests.size(); i++) {
			RedisRequest request = requests.get(i);
			RedisRequestPolicy policy = request.getPolicy();
			
			// 
			if ( CommandParse.MGETSET_CMD == policy.getLevel()
					|| (CommandParse.DEL_CMD == policy.getLevel()  && request.getArgs().length > 2 )
					|| (CommandParse.EXISTS_CMD == policy.getLevel() && request.getArgs().length > 2) ) {
				
				requestType = rewrite(request, newRequests, segments);

			} else {
				newRequests.add(request);
				int[] indexs = { newRequests.size() - 1 };

				Segment segment = new Segment(SegmentType.DEFAULT, indexs);
				segments.add(segment);
			}
		}

		List<RouteResultNode> nodes = doSharding(poolId, newRequests);
		requestType = requests.size() > 1 ? RedisRequestType.PIPELINE : requestType;

		RouteResult result = new RouteResult(requestType, newRequests, nodes);
		result.setSegments(segments);
		return result;
	}
}
