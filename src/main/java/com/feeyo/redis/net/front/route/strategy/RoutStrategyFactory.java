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
	
//	private static MGetSetRoutStrategy _MGETSET = new MGetSetRoutStrategy();
	private static MGetSetRoutStrategyV2 _MGETSET = new MGetSetRoutStrategyV2();
	
	// TODO del 多个key，在集群， pipeline有问题，后面修改
	private static DelMultiKeyRoutStrategy _DEL = new DelMultiKeyRoutStrategy();
	
    public static AbstractRouteStrategy getStrategy(int poolType, RedisRequestPolicy accuralRequestPolicy) {
    	
    	// 集群情况下，需要对 Mset、Mget、Del mulitKey 分片
    	if ( poolType == 1 || poolType == 2) {
    		if ( accuralRequestPolicy.getLevel() == CommandParse.MGETSET_CMD ) {
    			 return _MGETSET;
    			 
    		} else if ( accuralRequestPolicy.getLevel() == CommandParse.DEL_CMD ) {
    			 return _DEL;
    		}
    	}
    	
    	return _DEFAULT;
    }

}
