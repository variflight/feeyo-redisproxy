package com.feeyo.redis.net.front.route;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategy;
import com.feeyo.redis.net.front.prefix.KeyPrefixStrategyFactory;
import com.feeyo.redis.net.front.route.strategy.AbstractRouteStrategy;
import com.feeyo.redis.net.front.route.strategy.RoutStrategyFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 路由功能
 * @author yangtao
 *
 */
public class RouteService {
	
	// 路由计算, 必须认证后
	public static RouteResult route(List<RedisRequest> requests, RedisFrontConnection frontCon) 
			throws InvalidRequestExistsException, ManageRespNotTransException, AutoRespNotTransException, PhysicalNodeUnavailableException {
		
		int poolId = frontCon.getUserCfg().getPoolId();
		int poolType = frontCon.getUserCfg().getPoolType();
		boolean isReadOnly = frontCon.getUserCfg().isReadonly();
		boolean isAdmin = frontCon.getUserCfg().isAdmin();
		boolean isPipeline = requests.size() > 1;

		List<RedisRequestPolicy> requestPolicys = new ArrayList<RedisRequestPolicy>( requests.size() );		// 指令策略
		List<Integer> autoResponseIndexs = new ArrayList<Integer>();										// 直接返回指令索引
		
		// 请求是否存在不合法
		boolean invalidPolicyExist = false;
		for(int i = 0; i < requests.size(); i++) {
			
			RedisRequest request = requests.get(i);
			if (request == null || request.getArgs().length == 0) {
				return null;
			}
			
			String cmd = new String( request.getArgs()[0] ).toUpperCase();
			RedisRequestPolicy requestPolicy = CommandParse.getPolicy( cmd );
			requestPolicys.add( requestPolicy );
			
			// 如果是管理指令，且非pipeline,且是管理员用户      提取跳出
			if ( !isPipeline && requestPolicy.getLevel() == CommandParse.MANAGE_CMD && isAdmin) {
				throw new ManageRespNotTransException("manage cmd exist", requests, requestPolicys);
			}
			
			// 如果上个指令是合法的，继续校验下个指令
			if ( !invalidPolicyExist ) {
				invalidPolicyExist = checkIsInvalidPolicy( poolType, requestPolicy, isReadOnly, isAdmin, isPipeline );
			}
					
			// 不需要透传，中间件自动回复
			if ( requestPolicy.getLevel() == CommandParse.AUTO_RESP_CMD 
					|| requestPolicy.getLevel() == CommandParse.MANAGE_CMD ) {
				autoResponseIndexs.add(i);
				continue;
			} 

			// 前缀构建 
			byte[] prefix = frontCon.getUserCfg().getPrefix();
			if (prefix != null) {
				KeyPrefixStrategy strategy = KeyPrefixStrategyFactory.getStrategy(cmd);
				strategy.rebuildKey(request, prefix);
			}
		}
		
		// 存在不支持指令
		if ( invalidPolicyExist ) {
			throw new InvalidRequestExistsException("invalid policy exist", requests, requestPolicys);
		}
		
		// 全部自动回复
		if ( autoResponseIndexs.size() == requests.size() ) {
			throw new AutoRespNotTransException("auto response", requests, requestPolicys);
		}
		
		// 根据路由策略对查询请求做路由
		RedisRequestPolicy firstRequestPolicy = requestPolicys.get(0);
		AbstractRouteStrategy routeStrategy = RoutStrategyFactory.getStrategy(poolType, firstRequestPolicy);
		RouteResult routeResult = routeStrategy.route(poolId, requests, requestPolicys, autoResponseIndexs);
		return routeResult;
	}

	
	// 校验指令是否为 不合法， true 为不合法
	private static boolean checkIsInvalidPolicy(int poolType, RedisRequestPolicy requestPolicy, boolean isReadOnly, boolean isAdmin,
			boolean isPipeline) {
		
		// readonly 只读权限后， 不能执行写入操作
		if ( isReadOnly && !requestPolicy.isRead()  ) {
			return true;
		}
		
		boolean result = false;
		switch( requestPolicy.getLevel()  ) {
		case CommandParse.DISABLED_CMD :
			result = true;
			break;
		case CommandParse.NO_CLUSTER_CMD:
			if ( poolType == 1 ) //集群
				result = true;
			break;
		case CommandParse.CLUSTER_CMD:
			if ( poolType == 0 ) //非集群
				result = true; 
			break;
		case CommandParse.PUBSUB_CMD:
		case CommandParse.MGETSET_CMD:
			if ( isPipeline )	// pipeline 不支持 mset/mget/pubsub
				result = true;
			break;
		case CommandParse.MANAGE_CMD:
			if ( isPipeline )
				result = true;
			if ( !isAdmin )
				result = true;
			break;
		case CommandParse.UNKNOW_CMD:
			result = true;
			break;
		}
		return result;
	}
}
