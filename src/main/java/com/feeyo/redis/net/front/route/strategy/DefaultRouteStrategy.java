package com.feeyo.redis.net.front.route.strategy;

import java.util.List;

import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.route.InvalidRequestException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteNode;

/**
 * default route strategy servers for no default command and pipeline command
 *
 * @author Tr!bf wangyamin@variflight.com
 * @author yangtao
 * @author xuwenfeng
 */
public class DefaultRouteStrategy extends AbstractRouteStrategy {

	@Override
	public RouteResult route(UserCfg userCfg, List<RedisRequest> requests)
			throws InvalidRequestException, PhysicalNodeUnavailableException {

		// 切片
		int poolId = userCfg.getPoolId();
		List<RouteNode> nodes = requestSharding(poolId, requests);

		RedisRequestType requestType;
		if (requests.size() == 1) {
			RedisRequest firstRequest = requests.get(0);
			requestType = firstRequest.getPolicy().getHandleType() == CommandParse.BLOCK_CMD ? RedisRequestType.BLOCK
					: RedisRequestType.DEFAULT;
		} else {
			requestType = RedisRequestType.PIPELINE;
		}

		RouteResult routeResult = new RouteResult(requestType, requests, nodes);
		return routeResult;
	}

}
