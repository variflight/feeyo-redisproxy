package com.feeyo.redis.net.front.route;

import com.feeyo.redis.net.backend.pool.PoolType;
import com.feeyo.redis.net.codec.RedisRequestPolicy;
import com.feeyo.redis.net.front.handler.CommandParse;

public class RouteUtil {
	
	// 校验指令是否为 不合法， true 为不合法
	public static boolean isInvalidRequest(int poolType, RedisRequestPolicy requestPolicy, 
			boolean isReadOnly, boolean isAdmin, boolean isPipeline) {
		
		// readonly 只读权限后， 不能执行写入操作
		if ( isReadOnly && !requestPolicy.isRead()  ) {
			return true;
		}
		
		// Kakfa 指令校验
		if ( poolType == PoolType.KAFKA_CLUSTER ) {
			
			if ( requestPolicy.getCategory() != CommandParse.KAFKA_CMD )
				return true;
			
			if (isPipeline)
				return true;  // kafka指令 不支持pipeline
			
		}  else {
			if ( requestPolicy.getCategory() == CommandParse.KAFKA_CMD ) 
				return true;
			
		}
		
		// 类型策略校验
		switch( requestPolicy.getCategory()  ) {
		case CommandParse.DISABLED_CMD :
			return true;
		case CommandParse.NO_CLUSTER_CMD:
			if ( poolType == PoolType.REDIS_CLUSTER ) //集群
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
		switch( requestPolicy.getHandleType() ) {
		case CommandParse.PUBSUB_CMD:
			if ( isPipeline )	// pipeline 不支持 pubsub
				return true;
			break;
		
		case CommandParse.BLOCK_CMD:
			if ( poolType == PoolType.REDIS_CLUSTER ) // 集群不支持阻塞指令
				return true;
			if ( isPipeline )
				return true;  // pipe不支持阻塞指令
			break;
		}
		return false;
	}

}
