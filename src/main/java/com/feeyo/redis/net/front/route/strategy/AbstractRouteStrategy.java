package com.feeyo.redis.net.front.route.strategy;

import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.backend.pool.cluster.ClusterCRC16Util;
import com.feeyo.redis.net.backend.pool.cluster.RedisClusterPool;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;

import java.util.ArrayList;
import java.util.List;

/**
 * The abstract route strategy
 *
 * @author Tr!bf wangyamin@variflight.com
 */
public abstract class AbstractRouteStrategy {
	
	
	// pipeline 分片
	protected List<RouteResultNode> doSharding(int poolId, 
			List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys) throws PhysicalNodeUnavailableException {
		
		List<RouteResultNode> nodes = new ArrayList<RouteResultNode>();
		
		// 非集群池
		AbstractPool pool = RedisEngineCtx.INSTANCE().getPoolMap().get( poolId );
		if ( pool.getType() == 0) {
			RouteResultNode node = new RouteResultNode();
			
			PhysicalNode physicalNode = pool.getPhysicalNode();
			if ( physicalNode == null )
				throw new PhysicalNodeUnavailableException("node unavailable.");
			node.setPhysicalNode( physicalNode );
			
			for(int i = 0; i < requests.size(); i++) {
				node.addRequestIndex(i);
			}
			node.setPhysicalNode(pool.getPhysicalNode());
			nodes.add( node );
			
		// 集群池
		} else if ( pool.getType() == 1) {
			
			RedisClusterPool clusterPool =  (RedisClusterPool) pool;
			for (int i = 0; i < requests.size(); i++) {
				
				if ( requestPolicys.get(i).getLevel() == RedisRequestPolicy.AUTO_RESP_CMD )  {
					continue;
				}
				
				// 计算key的slot值。
				int slot = 0;
				RedisRequest request = requests.get(i);
				byte[] requestKey = request.getNumArgs() > 1 ? request.getArgs()[1] : null;
				if (requestKey != null) {
					slot = ClusterCRC16Util.getSlot(requestKey, false);
				}
				
				// 根据 slot 获取 redis物理节点
				PhysicalNode physicalNode = clusterPool.getPhysicalNodeBySlot(slot) ;
				if ( physicalNode == null )
					throw new PhysicalNodeUnavailableException("node unavailable.");
				
				boolean isFind = false;
				for (RouteResultNode node: nodes) {
					if ( node.getPhysicalNode() == physicalNode ) {
						node.addRequestIndex(i);
						isFind = true;
						break;
					}
				}
				
				if ( !isFind ) {
					RouteResultNode node = new RouteResultNode();
					node.setPhysicalNode( physicalNode );
					node.addRequestIndex(i);
					nodes.add( node );
				}
			}
		}
		return nodes;
	}
	
	// 路由
    public abstract RouteResult route(int poolId, List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys, 
    		List<Integer> autoResponseIndexs) throws InvalidRequestExistsException, PhysicalNodeUnavailableException;

}
