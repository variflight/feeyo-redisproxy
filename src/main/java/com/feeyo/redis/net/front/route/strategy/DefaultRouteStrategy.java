package com.feeyo.redis.net.front.route.strategy;

import java.util.List;

import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;

/**
 * default route strategy servers for no default command and pipeline command
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class DefaultRouteStrategy extends AbstractRouteStrategy {
	
    @Override
    public RouteResult route(int poolId, List<RedisRequest> requests) 
    		throws InvalidRequestExistsException, PhysicalNodeUnavailableException {
    	
    	// 切片
        List<RouteResultNode> nodes = doSharding(poolId, requests);
        
		RedisRequestType requestType;
		if (requests.size() == 1) {
			requestType = requests.get(0).getPolicy().getHandleType() == CommandParse.BLOCK_CMD ? RedisRequestType.BLOCK
					: RedisRequestType.DEFAULT;
		} else {
			requestType = RedisRequestType.PIPELINE;
		}
        
        RouteResult routeResult = new RouteResult(requestType, requests, nodes);
        return routeResult;
    }

}
