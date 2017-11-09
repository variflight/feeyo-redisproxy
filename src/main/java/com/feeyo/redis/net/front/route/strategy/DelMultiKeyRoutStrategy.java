package com.feeyo.redis.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;

/**
 * DEL 多key 的改写
 */
public class DelMultiKeyRoutStrategy extends AbstractRouteStrategy {
	
	private void rewrite(RedisRequest firstRequest, RedisRequestPolicy firstRequestPolicy, 
			List<RedisRequest> newRequests, List<RedisRequestPolicy> newRequestPolicys) throws InvalidRequestExistsException {
		
        byte[][] args = firstRequest.getArgs();
        if (args.length == 1) {
            throw new InvalidRequestExistsException("wrong number of arguments", null, null);
        }
        
        for (int j=1; j<args.length; j++) {
            RedisRequest request = new RedisRequest();
            request.setArgs(new byte[][] {"DEL".getBytes(), args[j] });
            
            newRequests.add( request );
            newRequestPolicys.add( firstRequestPolicy );
        }
	}
	

	@Override
	public RouteResult route(int poolId, List<RedisRequest> requests,
			List<RedisRequestPolicy> requestPolicys, List<Integer> autoResponseIndexs)
			throws InvalidRequestExistsException, PhysicalNodeUnavailableException {
		
		RedisRequest firstRequest = requests.get(0);
        RedisRequestPolicy firstRequestPolicy = requestPolicys.get(0);
        byte[][] args = firstRequest.getArgs();
        
    	boolean isNoPipeline = ( requests.size() == 1 );
    	boolean isMultiKey = ( args.length > 2);
        
        // 集群, No PIPELINE、 MultiKey DEL
        if ( isNoPipeline && isMultiKey ) {
        	
    		List<RedisRequest> newRequests = new ArrayList<RedisRequest>( firstRequest.getNumArgs() - 1);
    		List<RedisRequestPolicy> newRequestPolicys = new ArrayList<RedisRequestPolicy>(  newRequests.size() );
    		
    		// 指令改写
    		rewrite( firstRequest, firstRequestPolicy, newRequests, newRequestPolicys );
    		    	
        	// 请求分片
            List<RouteResultNode> nodes = doSharding(poolId, newRequests, newRequestPolicys);
            RouteResult routeResult = new RouteResult(RedisRequestType.DEL_MULTIKEY, newRequests, newRequestPolicys, nodes, autoResponseIndexs,null);
            return routeResult;
        } 
        
        
        List<RouteResultNode> nodes = doSharding(poolId, requests, requestPolicys);
        RedisRequestType requestType = requests.size() == 1 ? RedisRequestType.DEFAULT : RedisRequestType.PIPELINE;
        RouteResult routeResult = new RouteResult(requestType, requests, requestPolicys, nodes, autoResponseIndexs,null);
        return routeResult;
	}

}
