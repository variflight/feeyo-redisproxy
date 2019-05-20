package com.feeyo.redis.net.front.route.strategy;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.net.codec.redis.RedisRequestPolicy;
import com.feeyo.net.codec.redis.RedisRequestType;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.handler.segment.Segment;
import com.feeyo.redis.net.front.handler.segment.SegmentType;
import com.feeyo.redis.net.front.route.InvalidRequestException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteNode;
import com.feeyo.redis.net.front.route.RouteResult;

import java.util.ArrayList;
import java.util.List;

/**
 * pipeline && mget and mset and del and exists and default command route
 * strategy
 *
 * @author xuwenfeng
 */
public class SegmentRouteStrategy extends AbstractRouteStrategy {
	

	private RedisRequestType rewrite(RedisRequest request, List<RedisRequest> newRequests, List<Segment> segments, byte[] expireTime )
			throws InvalidRequestException {

		byte[][] args = request.getArgs();
		
		// 此处待优化
		String cmd = new String(args[0]).toUpperCase();
		
		// mset 分包
		if (cmd.startsWith("MSET")) {
			
			if (args.length == 1 || (args.length & 0x01) == 0) {
				throw new InvalidRequestException("wrong number of arguments");
			}
			int[] indexs = new int[(args.length - 1) / 2];
			for (int j = 1; j < args.length; j += 2) {
				RedisRequest newRequest = new RedisRequest();
                newRequest.setArgs(new byte[][]{"SET".getBytes(), args[j], args[j + 1], "PX".getBytes(), expireTime});
				newRequest.setPolicy( request.getPolicy() );
				newRequests.add(newRequest);
	
				indexs[(j - 1) / 2] = newRequests.size() - 1;
			}
	
			Segment segment = new Segment(SegmentType.MSET, indexs);
			segments.add(segment);
			return RedisRequestType.MSET;

		} else if (cmd.startsWith("MGET")) {
			
			if (args.length == 1) {
	            throw new InvalidRequestException("wrong number of arguments", null);
	        }
	        int[] indexs = new int[args.length-1];
			for (int j = 1; j < args.length; j++) {
	            RedisRequest newRequest = new RedisRequest();
	            newRequest.setArgs(new byte[][] {"GET".getBytes(),args[j]});
	            newRequest.setPolicy( request.getPolicy() );
	            newRequests.add( newRequest );

	            indexs[j-1] = newRequests.size()-1;
	        }
			
			Segment segment = new Segment(SegmentType.MGET, indexs);
			segments.add(segment);
			
	        return RedisRequestType.MGET;
	        
		} else if (cmd.startsWith("DEL")) {
			
			if (args.length < 3) {
	            throw new InvalidRequestException("wrong number of arguments", null);
	        }
	    	int[] indexs = new int[args.length-1];
	        for (int j=1; j<args.length; j++) {
	            RedisRequest newRequest = new RedisRequest();
	            newRequest.setArgs(new byte[][] {"DEL".getBytes(), args[j] });
	            newRequest.setPolicy( request.getPolicy() );
	            newRequests.add( newRequest );

	            indexs[j-1] = newRequests.size()-1;
	        }
	        
	        Segment segment = new Segment(SegmentType.MDEL, indexs);
			segments.add(segment);
	    	return RedisRequestType.DEL_MULTIKEY;
			
		} else if (cmd.startsWith("EXISTS")) {
			
			if (args.length < 3) {
	            throw new InvalidRequestException("wrong number of arguments", null);
	        }
	    	int[] indexs = new int[args.length-1];
	        for (int j=1; j<args.length; j++) {
	            RedisRequest newRequest = new RedisRequest();
	            newRequest.setArgs(new byte[][] {"EXISTS".getBytes(), args[j] });
	            newRequest.setPolicy( request.getPolicy() );
	            newRequests.add( newRequest );

	            indexs[j-1] = newRequests.size()-1;
	        }
	        
	        Segment segment = new Segment(SegmentType.MEXISTS, indexs);
			segments.add(segment);
			return RedisRequestType.MEXISTS;
		}
		
		return null;
	}

	@Override
	public RouteResult route(UserCfg userCfg, List<RedisRequest> requests)
			throws InvalidRequestException, PhysicalNodeUnavailableException {

		List<Segment> segments = new ArrayList<Segment>();

		ArrayList<RedisRequest> newRequests = new ArrayList<RedisRequest>();

		RedisRequestType requestType = null;
		for (int i = 0; i < requests.size(); i++) {
			RedisRequest request = requests.get(i);
			RedisRequestPolicy policy = request.getPolicy();
			
			// 
			if ( CommandParse.MGETSET_CMD == policy.getHandleType()
					|| (CommandParse.DEL_CMD == policy.getHandleType()  && request.getArgs().length > 2 )
					|| (CommandParse.EXISTS_CMD == policy.getHandleType() && request.getArgs().length > 2)) {
                requestType = rewrite(request, newRequests, segments, userCfg.getKeyExpireTime());

			} else {
				newRequests.add(request);
				int[] indexs = { newRequests.size() - 1 };

				Segment segment = new Segment(SegmentType.DEFAULT, indexs);
				segments.add(segment);
			}
		}

		List<RouteNode> nodes = requestSharding(userCfg.getPoolId(), newRequests);
		requestType = requests.size() > 1 ? RedisRequestType.PIPELINE : requestType;

		RouteResult result = new RouteResult(requestType, newRequests, nodes);
		result.setSegments(segments);
		return result;
	}
}
