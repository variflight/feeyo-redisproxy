package com.feeyo.redis.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.pool.AbstractPool;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.backend.pool.PoolType;
import com.feeyo.redis.net.backend.pool.cluster.ClusterCRC16Util;
import com.feeyo.redis.net.backend.pool.cluster.RedisClusterPool;
import com.feeyo.redis.net.backend.pool.xcluster.XClusterPool;
import com.feeyo.redis.net.backend.pool.xcluster.XNodeUtil;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.front.route.InvalidRequestException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteNode;

/**
 * The abstract route strategy
 *
 * @author Tr!bf wangyamin@variflight.com
 * @author yangtao
 * @author xuwenfeng
 */
public abstract class AbstractRouteStrategy {
	
	
	// request list 分片
	protected List<RouteNode> requestSharding(int poolId, List<RedisRequest> requests) 
			throws PhysicalNodeUnavailableException {
		
		List<RouteNode> nodes = new ArrayList<RouteNode>();
		
		// 非集群池
		AbstractPool pool = RedisEngineCtx.INSTANCE().getPoolMap().get( poolId );
		if ( pool.getType() == PoolType.REDIS_STANDALONE ) {
			RouteNode node = new RouteNode();
			
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
		} else if ( pool.getType() == PoolType.REDIS_CLUSTER ) {
			
			RedisClusterPool clusterPool =  (RedisClusterPool) pool;
			for (int i = 0; i < requests.size(); i++) {
				
				RedisRequest request = requests.get(i);
				if ( request.getPolicy().isNotThrough() )  {
					continue;
				}

				// 计算key的slot值。
				int slot = 0;
				byte[] requestKey = request.getNumArgs() > 1 ? request.getArgs()[1] : null;
				if (requestKey != null) {
					slot = ClusterCRC16Util.getSlot( requestKey );
				}
				
				// 根据 slot 获取 redis物理节点
				PhysicalNode physicalNode = clusterPool.getPhysicalNodeBySlot(slot) ;
				if ( physicalNode == null )
					throw new PhysicalNodeUnavailableException("node unavailable.");

				allocateRequestIdxToPhysicalNode(nodes, i, physicalNode);
			}
		} else if ( pool.getType() == PoolType.REDIS_X_CLUSTER ) {
			
			XClusterPool xPool = (XClusterPool) pool;
			for (int i = 0; i < requests.size(); i++) {
				
				// 
				RedisRequest request = requests.get(i);
				if ( request.getPolicy().isNotThrough() )  {
					continue;
				}
				
				// 根据后缀 路由节点
				String suffix = XNodeUtil.getSuffix( request );
				PhysicalNode physicalNode = xPool.getPhysicalNode( suffix );
				if ( physicalNode == null )
					throw new PhysicalNodeUnavailableException("node unavailable.");
				
				allocateRequestIdxToPhysicalNode(nodes, i, physicalNode);
			}
		}
		
		return nodes;
	}

	// 分配 request index 给 physicalNode
	private void allocateRequestIdxToPhysicalNode(List<RouteNode> nodes, int requestIdx, PhysicalNode physicalNode) {
		boolean isNotFound = true;
		for (RouteNode node: nodes) {
			if ( node.getPhysicalNode() == physicalNode ) {
				node.addRequestIndex(requestIdx);
				isNotFound = false;
				break;
			}
		}

		if ( isNotFound ) {
			RouteNode node = new RouteNode();
			node.setPhysicalNode( physicalNode );
			node.addRequestIndex(requestIdx);
			nodes.add( node );
		}
	}

	// 路由
    public abstract RouteResult route(UserCfg userCfg, List<RedisRequest> requests) 
    		throws InvalidRequestException, PhysicalNodeUnavailableException;

}
