package com.feeyo.redis.net.front.route.strategy;

/**
 * route strategy factory
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class RoutStrategyFactory {
	
	private static DefaultRouteStrategy _DEFAULT = new DefaultRouteStrategy();
	
	private static MultiOperatorRoutStrategy  _MultiOP = new MultiOperatorRoutStrategy();
	
    public static AbstractRouteStrategy getStrategy(int poolType, boolean isMultiOpExist) {
    	
    	// 集群情况下，需要对 Mset、Mget、Del mulitKey 分片
    	if ((poolType == 1 || poolType == 2) && isMultiOpExist) {
    			 return _MultiOP;
    	}
    	
    	return _DEFAULT;
    }

}
