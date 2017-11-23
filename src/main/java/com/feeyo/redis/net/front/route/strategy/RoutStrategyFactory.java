package com.feeyo.redis.net.front.route.strategy;

/**
 * route strategy factory
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class RoutStrategyFactory {
	
	private static DefaultRouteStrategy _DEFAULT = new DefaultRouteStrategy();
	private static SegmentRoutStrategy  _SEGMENT = new SegmentRoutStrategy();
	
    public static AbstractRouteStrategy getStrategy(int poolType, boolean isNeedSegment) {
    	
    	// 集群情况下，需要对 Mset、Mget、Del mulitKey 分片
    	if ((poolType == 1 || poolType == 2) && isNeedSegment) {
    			 return _SEGMENT;
    	}
    	
    	return _DEFAULT;
    }

}
