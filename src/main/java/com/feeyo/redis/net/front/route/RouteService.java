package com.feeyo.redis.net.front.route;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategyFactory;
import com.feeyo.redis.net.front.route.strategy.AbstractRouteStrategy;
import com.feeyo.redis.net.front.route.strategy.DefaultRouteStrategy;
import com.feeyo.redis.net.front.route.strategy.SegmentRouteStrategy;

/**
 * 路由功能
 * @author yangtao
 *
 */
public class RouteService {
	
	private static DefaultRouteStrategy _DEFAULT = new DefaultRouteStrategy();
	private static SegmentRouteStrategy _SEGMENT = new SegmentRouteStrategy();
	
	
	private static AbstractRouteStrategy getStrategy(int poolType, boolean isNeedSegment) {
    	// 集群情况下，需要对 Mset、Mget、Del mulitKey 分片
    	switch( poolType ) {
    	case 1:
    	case 2:
    		if ( isNeedSegment )
    			 return _SEGMENT;
    		break;
    	}
    	return _DEFAULT;
    }
	
	// 路由计算, 必须认证后
	public static RouteResult route(List<RedisRequest> requests, RedisFrontConnection frontCon) 
			throws InvalidRequestExistsException, FullRequestNoThroughtException, PhysicalNodeUnavailableException {
		
		int poolId = frontCon.getUserCfg().getPoolId();
		int poolType = frontCon.getUserCfg().getPoolType();
		boolean isReadOnly = frontCon.getUserCfg().isReadonly();
		boolean isAdmin = frontCon.getUserCfg().isAdmin();
		boolean isPipeline = requests.size() > 1;

		List<Integer> noThroughtIndexs = null;
		boolean isNeedSegment = false;
		
		for(int i = 0; i < requests.size(); i++) {
			
			RedisRequest request = requests.get(i);
			if (request == null || request.getArgs().length == 0) {
				return null;
			}
			
			String cmd = new String( request.getArgs()[0] ).toUpperCase();
			RedisRequestPolicy policy = CommandParse.getPolicy( cmd );
			request.setPolicy( policy );
			
			// 包含批量操作命令，则采用分段的路由策略
			if(!isNeedSegment && ( 
					policy.getHandleType() == CommandParse.MGETSET_CMD 
					|| (policy.getHandleType() == CommandParse.DEL_CMD && request.getArgs().length > 2 )
					|| (policy.getHandleType() == CommandParse.EXISTS_CMD && request.getArgs().length > 2) ) ) {
				isNeedSegment = true;
			}
			
			
			// 是否存在无效指令
			boolean invalidRequestExist =  RouteUtil.checkIsInvalidPolicy( poolType, policy, isReadOnly, isAdmin, isPipeline );
			if ( invalidRequestExist ) {
				throw new InvalidRequestExistsException("invalid request exist");
			}
					
			// 不需要透传
			if ( policy.getHandleType() == CommandParse.NO_THROUGH_CMD ) {
				if ( noThroughtIndexs == null )
					noThroughtIndexs = new ArrayList<Integer>(2);
				noThroughtIndexs.add(i);
				continue;
			} 

			// 前缀构建 
			byte[] prefix = frontCon.getUserCfg().getPrefix();
			if (prefix != null) {
				KeyPrefixStrategy strategy = KeyPrefixStrategyFactory.getStrategy(cmd);
				strategy.rebuildKey(request, prefix);
			}
		}
		
		// 全部自动回复
		if ( noThroughtIndexs != null && noThroughtIndexs.size() == requests.size() ) {
			throw new FullRequestNoThroughtException("full request no throught", requests);
		}
		
		// 根据请求做路由
		AbstractRouteStrategy strategy = getStrategy(poolType, isNeedSegment);
		RouteResult routeResult = strategy.route(poolId, requests);
		if ( noThroughtIndexs != null )
			routeResult.setNoThroughtIndexs( noThroughtIndexs );
		
		return routeResult;
	}

}