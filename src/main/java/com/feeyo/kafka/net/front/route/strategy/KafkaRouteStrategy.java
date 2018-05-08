package com.feeyo.kafka.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.config.DataOffset;
import com.feeyo.kafka.config.DataPartition;
import com.feeyo.kafka.net.backend.pool.KafkaPool;
import com.feeyo.kafka.net.front.route.KafkaRouteNode;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteNode;
import com.feeyo.redis.net.front.route.strategy.AbstractRouteStrategy;

/**
 * Kafka command
 *
 * @author yangtao
 */
public class KafkaRouteStrategy extends AbstractRouteStrategy {
	
	
    @Override
    public RouteResult route(UserCfg userCfg, List<RedisRequest> requests) 
			throws InvalidRequestExistsException, PhysicalNodeUnavailableException {
    	
    	RedisRequest request = requests.get(0);
    	if ( request.getNumArgs() > 1 ) {
    		throw new InvalidRequestExistsException("wrong number of arguments");
    	}
    	
    	// 获取 topic
    	String topicName = new String( request.getArgs()[1] );
		TopicCfg topicCfg = RedisEngineCtx.INSTANCE().getKafkaTopicMap().get( topicName );
		if (topicCfg == null) {
			throw new InvalidRequestExistsException("topic not exists");
		}
		
		if (topicCfg.getMetadata() == null) {
			throw new InvalidRequestExistsException("topic not create or not load to kafka...");
		} 
		
		
		// 分区
		DataPartition partition = null;
	
		// 参数有效性校验
		switch( request.getPolicy().getHandleType() ) {
		case CommandParse.PRODUCE_CMD:
			{
				if (request.getNumArgs() != 3) {
					throw new InvalidRequestExistsException("wrong number of arguments");
				}
	
				if ( !topicCfg.isProducer( userCfg.getPassword() )) {
					throw new InvalidRequestExistsException("no authority");
				}
				
				partition = topicCfg.getMetadata().getProducerDataPartition();
			}
			
			break;
		case CommandParse.CONSUMER_CMD:
			{
				if (request.getNumArgs() != 2 && request.getNumArgs() != 4 ) {
					throw new InvalidRequestExistsException("wrong number of arguments");
				}
				
				if ( !topicCfg.isConsumer( userCfg.getPassword() )) {
					throw new InvalidRequestExistsException("no authority");
				}
				
				if (request.getNumArgs() == 4) {
					int pt = Integer.parseInt(new String(request.getArgs()[2]));
					partition = topicCfg.getMetadata().getConsumerDataPartition(pt);

				} else {
					partition = topicCfg.getMetadata().getConsumerDataPartition();
				}
				
			}
			break;
		}

		if ( partition == null ) {
			throw new InvalidRequestExistsException("wrong partition");
		}
		
		DataOffset dataOffset = topicCfg.getMetadata().getDataOffsetByPartition( partition.getPartition() );
		
		//
		KafkaPool pool = (KafkaPool) RedisEngineCtx.INSTANCE().getPoolMap().get( topicCfg.getPoolId() );
		PhysicalNode physicalNode = pool.getPhysicalNode( partition.getLeader().getId() );
		if (physicalNode == null)
			throw new PhysicalNodeUnavailableException("node unavailable.");
		
		KafkaRouteNode node = new KafkaRouteNode();
		node.setPhysicalNode(physicalNode);
		node.addRequestIndex(0);
		node.setDataOffset( dataOffset );

		List<RouteNode> nodes = new ArrayList<RouteNode>(1);
		nodes.add(node);

		RouteResult routeResult = new RouteResult(RedisRequestType.KAFKA, requests, nodes);
		return routeResult;
	}
}
