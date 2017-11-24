package com.feeyo.redis.net.front.subpackage.strategy;

import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.ext.Segment;
import com.feeyo.redis.net.front.handler.ext.SegmentType;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;

public class MDelSubPackageStrategy extends SubPackageStrategy {

	@Override
	public RedisRequestType subPackage(RedisRequest request, RedisRequestPolicy requestPolicy,
			List<RedisRequest> newRequests, List<RedisRequestPolicy> newRequestPolicys, List<Segment> segments)
			throws InvalidRequestExistsException {
		byte[][] args = request.getArgs();
		if (args.length < 3) {
            throw new InvalidRequestExistsException("wrong number of arguments", null, null);
        }
    	int[] indexs = new int[args.length-1];
        for (int j=1; j<args.length; j++) {
            RedisRequest newRequest = new RedisRequest();
            newRequest.setArgs(new byte[][] {"DEL".getBytes(), args[j] });
            
            newRequests.add( newRequest );
            newRequestPolicys.add( requestPolicy );
            indexs[j-1] = newRequests.size()-1;
        }
        
        Segment segment = new Segment(SegmentType.MDEL, indexs);
		segments.add(segment);
    	return RedisRequestType.DEL_MULTIKEY;
	}

}