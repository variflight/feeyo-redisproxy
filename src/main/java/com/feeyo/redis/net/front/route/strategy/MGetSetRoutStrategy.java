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
 * mget and mset command route strategy
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class MGetSetRoutStrategy extends AbstractRouteStrategy {
	
	private void rewrite(RedisRequest firstRequest, RedisRequestPolicy firstRequestPolicy, 
			List<RedisRequest> newRequests, List<RedisRequestPolicy> newRequestPolicys) throws InvalidRequestExistsException {
		
		byte[][] args = firstRequest.getArgs();
        String cmd = new String( args[0] ).toUpperCase();
        if (cmd.startsWith("MGET")) {
        	
            if (args.length == 1) {
                throw new InvalidRequestExistsException("wrong number of arguments", null, null);
            }
            
			for (int j = 1; j < args.length; j++) {
                RedisRequest request = new RedisRequest();
                request.setArgs(new byte[][] {"GET".getBytes(),args[j]});
                
                newRequests.add( request );
                newRequestPolicys.add( firstRequestPolicy );
            }
            
        } else {
        	
            if (args.length == 1 || (args.length & 0x01) == 0) {
                throw new InvalidRequestExistsException("wrong number of arguments", null, null);
            }

			for (int j = 1; j < args.length; j += 2) {
                RedisRequest request = new RedisRequest();
                request.setArgs(new byte[][] {"SET".getBytes(),args[j],args[j+1]});
                newRequests.add( request );
                newRequestPolicys.add( firstRequestPolicy );
            }
        }
	}
   
	@Override
    public RouteResult route(int poolId, List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys, 
    		List<Integer> autoResponseIndexs) throws InvalidRequestExistsException, PhysicalNodeUnavailableException {
	
		RedisRequest firstRequest = requests.get(0);
        RedisRequestPolicy firstRequestPolicy = requestPolicys.get(0);
        
        // 新的请求
		List<RedisRequest> newRequests = new ArrayList<RedisRequest>( firstRequest.getNumArgs() - 1);
		List<RedisRequestPolicy> newRequestPolicys = new ArrayList<RedisRequestPolicy>(  newRequests.size() );

    	// 指令改写
		rewrite( firstRequest, firstRequestPolicy, newRequests, newRequestPolicys );
    	
        List<RouteResultNode> nodes = doSharding(poolId, newRequests, newRequestPolicys);
        
        RouteResult routeResult;
        String cmd = new String( firstRequest.getArgs()[0] ).toUpperCase();
        if (cmd.startsWith("MGET")) {
            routeResult = new RouteResult(RedisRequestType.MGET, newRequests, newRequestPolicys, nodes, new ArrayList<Integer>(),null);    
        } else {
            routeResult = new RouteResult(RedisRequestType.MSET, newRequests, newRequestPolicys, nodes, new ArrayList<Integer>(),null);    
        }
        return routeResult;
    }

}
