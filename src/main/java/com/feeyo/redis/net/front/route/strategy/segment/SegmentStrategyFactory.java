package com.feeyo.redis.net.front.route.strategy.segment;

public class SegmentStrategyFactory {
	
	private static MSetSegmentStrategy MSET_STR =  new MSetSegmentStrategy();
	private static MGetSegmentStrategy MGET_STR =  new MGetSegmentStrategy();
	private static MDelSegmentStrategy MDEL_STR =  new MDelSegmentStrategy();
	private static MExistsSegmentStrategy EXISTS_STR =  new MExistsSegmentStrategy();
	

	public static SegmentStrategy getStrategy(String cmd) {
		if (cmd.startsWith("MSET"))
			return MSET_STR;
		else if (cmd.startsWith("MGET"))
			return MGET_STR;
		else if (cmd.startsWith("DEL"))
			return MDEL_STR;
		else if (cmd.startsWith("EXISTS"))
			return EXISTS_STR;
		return null;
	}

}
