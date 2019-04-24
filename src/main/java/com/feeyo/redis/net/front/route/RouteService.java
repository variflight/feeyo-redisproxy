package com.feeyo.redis.net.front.route;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.kafka.net.front.route.strategy.KafkaRouteStrategy;
import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.net.codec.redis.RedisRequestPolicy;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.net.backend.pool.PoolType;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.rewrite.KeyIllegalException;
import com.feeyo.redis.net.front.rewrite.KeyRewriteStrategy;
import com.feeyo.redis.net.front.rewrite.KeyRewriteStrategyFactory;
import com.feeyo.redis.net.front.route.strategy.AbstractRouteStrategy;
import com.feeyo.redis.net.front.route.strategy.DefaultRouteStrategy;
import com.feeyo.redis.net.front.route.strategy.SegmentRouteStrategy;

/**
 * 路由功能
 * 
 * @author yangtao
 *
 */
public class RouteService {
	
	private static DefaultRouteStrategy _DEFAULT = new DefaultRouteStrategy();
	private static SegmentRouteStrategy _SEGMENT = new SegmentRouteStrategy();
	private static KafkaRouteStrategy _KAFKA = new KafkaRouteStrategy();
	
	
	private static AbstractRouteStrategy getStrategy(int poolType, boolean isNeedSegment) {
		
		// 集群情况下，需要对 Mset、Mget、Del mulitKey 分片
		switch (poolType) {
		case PoolType.REDIS_STANDALONE:
			return _DEFAULT;
			
		case PoolType.REDIS_CLUSTER:
		case PoolType.REDIS_X_CLUSTER:
			if ( isNeedSegment )
				return _SEGMENT;
			break;
			
		case PoolType.KAFKA_CLUSTER:
			return _KAFKA;
		}
		return _DEFAULT;
	}
	
	// 路由计算, 必须认证后
	public static RouteResult route(List<RedisRequest> requests, RedisFrontConnection frontCon)
			throws InvalidRequestException, FullRequestNoThroughtException, PhysicalNodeUnavailableException,
			KeyIllegalException {
		
		UserCfg userCfg = frontCon.getUserCfg();
		
		int poolType = userCfg.getPoolType();
		boolean isReadOnly = userCfg.isReadonly();
		boolean isAdmin = userCfg.isAdmin();
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
			
			// 是否存在无效指令
			boolean invalidRequestExist =  RouteUtil.isInvalidRequest( poolType, policy, isReadOnly, isAdmin, isPipeline );
			if ( invalidRequestExist ) {
				throw new InvalidRequestException("invalid request exist");
			}
			
			// 不需要透传
			if ( policy.getHandleType() == CommandParse.NO_THROUGH_CMD ) {
				if ( noThroughtIndexs == null )
					noThroughtIndexs = new ArrayList<Integer>(2);
				noThroughtIndexs.add(i);
				continue;
			} 
			
			// 包含批量操作命令，则采用分段的路由策略
			if(!isNeedSegment && ( 
					policy.getHandleType() == CommandParse.MGETSET_CMD 
					|| (policy.getHandleType() == CommandParse.DEL_CMD && request.getArgs().length > 2 )
					|| (policy.getHandleType() == CommandParse.EXISTS_CMD && request.getArgs().length > 2) ) ) {
				isNeedSegment = true;
			}
						
			// 前缀、默认值 改写策略 
			KeyRewriteStrategy strategy = KeyRewriteStrategyFactory.getStrategy(cmd);
			strategy.rebuildKey(request, userCfg);
		}
		
		// 全部自动回复
		if ( noThroughtIndexs != null && noThroughtIndexs.size() == requests.size() ) {
			throw new FullRequestNoThroughtException("full request no throught", requests);
		}
		
		// 根据请求做路由
		AbstractRouteStrategy strategy = getStrategy(poolType, isNeedSegment);
		RouteResult routeResult = strategy.route(userCfg, requests);
		if ( noThroughtIndexs != null )
			routeResult.setNoThroughtIndexs( noThroughtIndexs );
		
		return routeResult;
	}

}