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
			if ( isPipeline )	// pipeline 不支持 pubsub
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
