package com.feeyo.redis.net.front.route.strategy.segment;

import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.segment.Segment;
import com.feeyo.redis.net.front.handler.segment.SegmentType;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;

public class MExistsSegmentStrategy extends SegmentStrategy{

	@Override
	public RedisRequestType unpack(RedisRequest request, RedisRequestPolicy requestPolicy, 
			List<RedisRequest> newRequests, List<RedisRequestPolicy> newRequestPolicys, 
			List<Segment> segments) throws InvalidRequestExistsException {
		
		byte[][] args = request.getArgs();
		if (args.length < 3) {
            throw new InvalidRequestExistsException("wrong number of arguments", null, null);
        }
    	int[] indexs = new int[args.length-1];
        for (int j=1; j<args.length; j++) {
            RedisRequest newRequest = new RedisRequest();
            newRequest.setArgs(new byte[][] {"EXISTS".getBytes(), args[j] });
            
            newRequests.add( newRequest );
            newRequestPolicys.add( requestPolicy );
            indexs[j-1] = newRequests.size()-1;
        }
        
        Segment segment = new Segment(SegmentType.MEXISTS, indexs);
		segments.add(segment);
		return RedisRequestType.MEXISTS;
	}

}
