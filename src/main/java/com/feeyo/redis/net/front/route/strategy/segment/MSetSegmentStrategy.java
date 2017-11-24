package com.feeyo.redis.net.front.route.strategy.segment;

import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.segment.Segment;
import com.feeyo.redis.net.front.handler.segment.SegmentType;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;

public class MSetSegmentStrategy extends SegmentStrategy {

	@Override
	public RedisRequestType unpack(RedisRequest request,
			List<RedisRequest> newRequests, List<Segment> segments)
			throws InvalidRequestExistsException {
		byte[][] args = request.getArgs();
		if (args.length == 1 || (args.length & 0x01) == 0) {
			throw new InvalidRequestExistsException("wrong number of arguments", null, null);
		}
		int[] indexs = new int[(args.length - 1) / 2];
		for (int j = 1; j < args.length; j += 2) {
			RedisRequest newRequest = new RedisRequest();
			newRequest.setArgs(new byte[][] { "SET".getBytes(), args[j], args[j + 1] });
			newRequest.setPolicy( request.getPolicy() );
			newRequests.add(newRequest);

			indexs[(j - 1) / 2] = newRequests.size() - 1;
		}

		Segment segment = new Segment(SegmentType.MSET, indexs);
		segments.add(segment);
		return RedisRequestType.MSET;
	}

}
