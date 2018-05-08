package com.feeyo.kafka.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.kafka.config.TopicCfg;
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
    	String topic = new String( request.getArgs()[1] );
		TopicCfg topicCfg = RedisEngineCtx.INSTANCE().getKafkaTopicMap().get(topic);
		if (topicCfg == null) {
			throw new InvalidRequestExistsException("topic not exists");
		}
		
		if (topicCfg.getMetaData() == null) {
			throw new InvalidRequestExistsException("topic not create or not load to kafka...");
		} 
    	

		//
		DataPartition partition = null;
		
		if ( request.getPolicy().getHandleType() == CommandParse.PRODUCE_CMD) {
			if (request.getNumArgs() != 3) {
				throw new InvalidRequestExistsException("wrong number of arguments");
			}

			if ( !topicCfg.isProducer( userCfg.getPassword() )) {
				throw new InvalidRequestExistsException("no authority");
			}
			
			partition = topicCfg.getMetaData().getProducerMetaDataPartition();
			
		} else {
			
			if (request.getNumArgs() != 2 && request.getNumArgs() != 4 ) {
				throw new InvalidRequestExistsException("wrong number of arguments");
			}
			
			if ( !topicCfg.isConsumer( userCfg.getPassword() )) {
				throw new InvalidRequestExistsException("no authority");
			}
			
			if (request.getNumArgs() == 4) {
				int pt = Integer.parseInt(new String(request.getArgs()[2]));
				partition = topicCfg.getMetaData().getConsumerMetaDataPartition(pt);
				if (partition == null) {
					throw new InvalidRequestExistsException("wrong partition");
				}
			} else {
				partition = topicCfg.getMetaData().getConsumerMetaDataPartition();
			}
		}

		//
		KafkaPool pool = (KafkaPool) RedisEngineCtx.INSTANCE().getPoolMap().get(topicCfg.getPoolId());
		PhysicalNode physicalNode = pool.getPhysicalNode(partition.getLeader().getId());
		if (physicalNode == null)
			throw new PhysicalNodeUnavailableException("node unavailable.");
		
		KafkaRouteNode node = new KafkaRouteNode();
		node.setPhysicalNode(physicalNode);
		node.addRequestIndex(0);
		node.setDataOffset( topicCfg.getMetaData().getMetaDataOffsetByPartition(partition.getPartition()) );

		List<RouteNode> nodes = new ArrayList<RouteNode>(1);
		nodes.add(node);

		RouteResult routeResult = new RouteResult(RedisRequestType.KAFKA, requests, nodes);
		return routeResult;
	}
}
