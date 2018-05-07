package com.feeyo.redis.net.front.route.strategy;

import java.util.List;

import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteNode;

/**
 * default route strategy servers for no default command and pipeline command
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class DefaultRouteStrategy extends AbstractRouteStrategy {
	
    @Override
    public RouteResult route(UserCfg userCfg, List<RedisRequest> requests) 
    		throws InvalidRequestExistsException, PhysicalNodeUnavailableException {
    	
    		int poolId = userCfg.getPoolId();
    		// 切片
        List<RouteNode> nodes = doSharding(poolId, requests);
        
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
