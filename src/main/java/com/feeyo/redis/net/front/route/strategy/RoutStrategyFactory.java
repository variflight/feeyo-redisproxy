package com.feeyo.redis.net.front.route.strategy;

import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.net.front.handler.CommandParse;

/**
 * route strategy factory
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public class RoutStrategyFactory {
	
	private static DefaultRouteStrategy _DEFAULT = new DefaultRouteStrategy();
	
	private static MGetSetRoutStrategy _MGETSET = new MGetSetRoutStrategy();
	private static DelMultiKeyRoutStrategy _DEL = new DelMultiKeyRoutStrategy();
	
    public static AbstractRouteStrategy getStrategy(int poolType, RedisRequestPolicy firstRequestPolicy) {
    	
    	// 集群情况下，需要对 Mset、Mget、Del mulitKey 分片
    	if ( poolType == 1 || poolType == 2) {
    		if ( firstRequestPolicy.getLevel() == CommandParse.MGETSET_CMD ) {
    			 return _MGETSET;
    			 
    		} else if ( firstRequestPolicy.getLevel() == CommandParse.DEL_CMD ) {
    			 return _DEL;
    		}
    	}
    	
    	return _DEFAULT;
    }

}
