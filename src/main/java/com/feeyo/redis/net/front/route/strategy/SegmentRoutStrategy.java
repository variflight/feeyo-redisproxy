package com.feeyo.redis.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.handler.ext.Segment;
import com.feeyo.redis.net.front.handler.ext.SegmentType;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;

/**
 * pipeline && mget and mset and default command route strategy
 *
 * @author xuwenfeng
 */
public class SegmentRoutStrategy extends AbstractRouteStrategy {
	
	private RedisRequestType rewrite(RedisRequest firstRequest, RedisRequestPolicy firstRequestPolicy, 
			List<RedisRequest> newRequests, List<RedisRequestPolicy> newRequestPolicys, 
			List<Segment> segments) throws InvalidRequestExistsException {
		
		byte[][] args = firstRequest.getArgs();
        String cmd = new String( args[0] ).toUpperCase();
        if (cmd.startsWith("MGET")) {
        	
            if (args.length == 1) {
                throw new InvalidRequestExistsException("wrong number of arguments", null, null);
            }
            int[] indexs = new int[args.length-1];
			for (int j = 1; j < args.length; j++) {
                RedisRequest request = new RedisRequest();
                request.setArgs(new byte[][] {"GET".getBytes(),args[j]});
                newRequests.add( request );
                newRequestPolicys.add( firstRequestPolicy );
                indexs[j-1] = newRequests.size()-1;
            }
			
			Segment segment = new Segment(SegmentType.MGET, indexs);
			segments.add(segment);
			
            return RedisRequestType.MGET;
            
        } else if(cmd.startsWith("MSET")){
        	
            if (args.length == 1 || (args.length & 0x01) == 0) {
                throw new InvalidRequestExistsException("wrong number of arguments", null, null);
            }
            int[] indexs = new int[(args.length-1)/2];
			for (int j = 1; j < args.length; j += 2) {
                RedisRequest request = new RedisRequest();
                request.setArgs(new byte[][] {"SET".getBytes(),args[j],args[j+1]});
                newRequests.add( request );
                newRequestPolicys.add( firstRequestPolicy );
                indexs[(j-1)/2] = newRequests.size()-1;
            }
			
			Segment segment = new Segment(SegmentType.MSET, indexs);
			segments.add(segment);
			return RedisRequestType.MSET;
			
        }else {
        	if (args.length < 3) {
                throw new InvalidRequestExistsException("wrong number of arguments", null, null);
            }
        	int[] indexs = new int[args.length-1];
            for (int j=1; j<args.length; j++) {
                RedisRequest request = new RedisRequest();
                request.setArgs(new byte[][] {"DEL".getBytes(), args[j] });
                
                newRequests.add( request );
                newRequestPolicys.add( firstRequestPolicy );
                indexs[j-1] = newRequests.size()-1;
            }
            
            Segment segment = new Segment(SegmentType.MDEL, indexs);
			segments.add(segment);
        	return RedisRequestType.DEL_MULTIKEY;
        }
	}
   
	@Override
    public RouteResult route(int poolId, List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys, 
    		List<Integer> autoResponseIndexs) throws InvalidRequestExistsException, PhysicalNodeUnavailableException {
		
		List<Segment> segments = new ArrayList<Segment>();
		
		ArrayList<RedisRequest> newRequests = new ArrayList<RedisRequest>(); 
		ArrayList<RedisRequestPolicy> newRequestPolicys = new ArrayList<RedisRequestPolicy>();
		
		RedisRequestType requestType = null;
		for(int i = 0; i<requests.size(); i++) {
			 RedisRequest request = requests.get(i);
			 RedisRequestPolicy requestPolicy = requestPolicys.get(i);
			if ( CommandParse.MGETSET_CMD == requestPolicy.getLevel()
					|| (CommandParse.DEL_CMD == requestPolicy.getLevel() && request.getArgs().length > 2)) {
				 
				 requestType = rewrite( request, requestPolicy, newRequests, newRequestPolicys, segments);
				 
			 }else {
				 newRequests.add(request);
				 newRequestPolicys.add(requestPolicy);
				 int[] indexs = {newRequests.size()-1};
				 
				 Segment segment = new Segment(SegmentType.DEFAULT, indexs);
				 segments.add(segment);
			 }
		}
		
		List<RouteResultNode> nodes = doSharding(poolId, newRequests, newRequestPolicys);
		requestType = requests.size() > 1 ? RedisRequestType.PIPELINE : requestType;
		return new RouteResult(requestType, newRequests, newRequestPolicys, nodes, autoResponseIndexs, segments);
    }
}
