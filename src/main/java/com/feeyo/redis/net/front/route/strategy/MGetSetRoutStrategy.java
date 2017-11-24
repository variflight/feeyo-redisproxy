package com.feeyo.redis.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
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
	
	private void rewrite(RedisRequest firstRequest, List<RedisRequest> newRequests) throws InvalidRequestExistsException {
		
		byte[][] args = firstRequest.getArgs();
        String cmd = new String( args[0] ).toUpperCase();
        if (cmd.startsWith("MGET")) {
        	
            if (args.length == 1) {
                throw new InvalidRequestExistsException("wrong number of arguments", null, null);
            }
            
			for (int j = 1; j < args.length; j++) {
                RedisRequest request = new RedisRequest();
                request.setArgs(new byte[][] {"GET".getBytes(),args[j]});
                request.setPolicy( firstRequest.getPolicy() );
                newRequests.add( request );
            }
            
        } else {
        	
            if (args.length == 1 || (args.length & 0x01) == 0) {
                throw new InvalidRequestExistsException("wrong number of arguments", null, null);
            }

			for (int j = 1; j < args.length; j += 2) {
                RedisRequest request = new RedisRequest();
                request.setArgs(new byte[][] {"SET".getBytes(),args[j],args[j+1]});
                request.setPolicy( firstRequest.getPolicy() );
                newRequests.add( request );
            }
        }
	}
   
	@Override
    public RouteResult route(int poolId, List<RedisRequest> requests) 
    		throws InvalidRequestExistsException, PhysicalNodeUnavailableException {
	
		RedisRequest firstRequest = requests.get(0);
        
        // 新的请求
		List<RedisRequest> newRequests = new ArrayList<RedisRequest>( firstRequest.getNumArgs() - 1);

    	// 指令改写
		rewrite( firstRequest, newRequests );
    	
        List<RouteResultNode> nodes = doSharding(poolId, newRequests );
        
        RouteResult routeResult;
        String cmd = new String( firstRequest.getArgs()[0] ).toUpperCase();
        
        routeResult = new RouteResult( cmd.startsWith("MGET") ? RedisRequestType.MGET: RedisRequestType.MSET, 
        		newRequests, nodes);    
        return routeResult;
    }

}
