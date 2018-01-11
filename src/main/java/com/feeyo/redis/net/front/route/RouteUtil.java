package com.feeyo.redis.net.front.route;

import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.net.front.handler.CommandParse;

public class RouteUtil {
	
	// 校验指令是否为 不合法， true 为不合法
	public static boolean checkIsInvalidPolicy(int poolType, RedisRequestPolicy requestPolicy, 
			boolean isReadOnly, boolean isAdmin, boolean isPipeline) {
		
		// readonly 只读权限后， 不能执行写入操作
		if ( isReadOnly && !requestPolicy.isRead()  ) {
			return true;
		}
		
		// 类型策略校验
		switch( requestPolicy.getTypePolicy()  ) {
		case CommandParse.DISABLED_CMD :
			return true;
		case CommandParse.NO_CLUSTER_CMD:
			if ( poolType == 1 ) //集群
				return true;
			break;
		case CommandParse.MANAGE_CMD:
			if ( isPipeline )
				return true;
			if ( !isAdmin )
				return true;
			break;
		case CommandParse.UNKNOW_CMD:
			return true;
		}
		
		// 处理策略校验
		switch( requestPolicy.getHandlePolicy() ) {
		case CommandParse.PUBSUB_CMD:
			if ( isPipeline )	// pipeline 不支持 pubsub
				return true;
			break;
		
		case CommandParse.BLOCK_CMD:
			if ( poolType == 1 ) // 集群不支持阻塞指令
				return true;
			if ( isPipeline )
				return true;  // pipe不支持阻塞指令
			break;
		}
		return false;
	}

}
