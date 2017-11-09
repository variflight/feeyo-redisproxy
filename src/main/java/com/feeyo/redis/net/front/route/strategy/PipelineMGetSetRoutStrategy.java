package com.feeyo.redis.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RequestIndexCombination;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResult.PIPELINE_COMMAND_TYPE;
import com.feeyo.redis.net.front.route.RouteResultNode;

/**
 * pipeline && mget and mset and default command route strategy
 *
 * @author xuwenfeng
 */
public class PipelineMGetSetRoutStrategy extends MGetSetRoutStrategy {
	
	private void rewrite(RedisRequest firstRequest, RedisRequestPolicy firstRequestPolicy, 
			List<RedisRequest> newRequests, List<RedisRequestPolicy> newRequestPolicys, List<RequestIndexCombination> requestIndexCombinations) throws InvalidRequestExistsException {
		
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
			RequestIndexCombination requestIndexCombination = new RequestIndexCombination(PIPELINE_COMMAND_TYPE.MGET_OP_COMMAND, indexs);
			requestIndexCombinations.add(requestIndexCombination);
            
        } else {
        	
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
			RequestIndexCombination requestIndexCombination = new RequestIndexCombination(PIPELINE_COMMAND_TYPE.MSET_OP_COMMAND, indexs);
			requestIndexCombinations.add(requestIndexCombination);
        }
	}
   
	@Override
    public RouteResult route(int poolId, List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys, 
    		List<Integer> autoResponseIndexs) throws InvalidRequestExistsException, PhysicalNodeUnavailableException {
		List<RequestIndexCombination> requestIndexCombinations = new ArrayList<RequestIndexCombination>();
		ArrayList<RedisRequest> newRequests = new ArrayList<RedisRequest>(); 
		ArrayList<RedisRequestPolicy> newRequestPolicys = new ArrayList<RedisRequestPolicy>(); 
		for(int i = 0; i<requests.size(); i++) {
			 RedisRequest request = requests.get(i);
			 RedisRequestPolicy requestPolicy = requestPolicys.get(i);
			 if(CommandParse.MGETSET_CMD == requestPolicy.getLevel()) {
				 rewrite( request, requestPolicy, newRequests, newRequestPolicys,requestIndexCombinations);
			 }else {
				 newRequests.add(request);
				 newRequestPolicys.add(requestPolicy);
				 int[] indexs = {newRequests.size()-1};
				 RequestIndexCombination requestIndexCombination = new RequestIndexCombination(PIPELINE_COMMAND_TYPE.DEFAULT_OP_COMMAND, indexs);
				 requestIndexCombinations.add(requestIndexCombination);
			 }
		}
		List<RouteResultNode> nodes = doSharding(poolId, newRequests, newRequestPolicys);
		return new RouteResult(RedisRequestType.PIPELINE, newRequests, newRequestPolicys, nodes, new ArrayList<Integer>(),requestIndexCombinations);
    }
}
