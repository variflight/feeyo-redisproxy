package com.feeyo.redis.net.front.route.strategy;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;

import java.util.List;

/**
 * default route strategy servers for no default command and pipeline command
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class DefaultRouteStrategy extends AbstractRouteStrategy {
	
    @Override
    public RouteResult route(int poolId, List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys ) 
    		throws InvalidRequestExistsException, PhysicalNodeUnavailableException {
    	
    	// 切片
        List<RouteResultNode> nodes = doSharding(poolId, requests, requestPolicys);
        
        RedisRequestType requestType = requests.size() == 1 ? RedisRequestType.DEFAULT : RedisRequestType.PIPELINE;
        RouteResult routeResult = new RouteResult(requestType, requests, requestPolicys, nodes);
        return routeResult;
    }

}
