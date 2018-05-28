package com.feeyo.kafka.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.net.backend.broker.BrokerPartition;
import com.feeyo.kafka.net.backend.broker.offset.KafkaOffsetService;
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
import com.feeyo.redis.net.front.route.RouteNode;
import com.feeyo.redis.net.front.route.RouteResult;
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
    	if ( request.getNumArgs() < 2 ) {
    		throw new InvalidRequestExistsException("wrong number of arguments");
    	}
    	
    	// 获取 topic
    	String topicName = new String( request.getArgs()[1] );
    		KafkaPoolCfg kafkaPoolCfg = (KafkaPoolCfg) RedisEngineCtx.INSTANCE().getPoolCfgMap().get( userCfg.getPoolId() );
    		if (kafkaPoolCfg == null) {
    			throw new InvalidRequestExistsException("kafka pool not exists");
    		}
    		
		TopicCfg topicCfg = kafkaPoolCfg.getTopicCfgMap().get( topicName );
		if (topicCfg == null) {
			throw new InvalidRequestExistsException("topic not exists");
		}
		
		if (topicCfg.getRunningInfo() == null) {
			throw new InvalidRequestExistsException("topic not create or not load to kafka...");
		} 
		
		// 分区
		BrokerPartition partition = null;
		long offset = -1;
		int maxBytes = 1;
	
		// 参数有效性校验
		switch( request.getPolicy().getHandleType() ) {
		case CommandParse.PRODUCE_CMD:
			{
				if (request.getNumArgs() != 3 && request.getNumArgs() != 4) {
					throw new InvalidRequestExistsException("wrong number of arguments");
				}
	
				if ( !topicCfg.isProducer( userCfg.getPassword() )) {
					throw new InvalidRequestExistsException("no authority");
				}
				
				// 轮询分区生产
				if (request.getNumArgs() == 3) {
					partition = topicCfg.getRunningInfo().getPartitionByProducer();
					
				// 指定分区生产
				} else {
					int pt = Integer.parseInt(new String(request.getArgs()[3]));
					partition = topicCfg.getRunningInfo().getPartition(pt);
				} 
			}
			
			break;
		case CommandParse.CONSUMER_CMD:
			{
				if (request.getNumArgs() != 2 && request.getNumArgs() != 4 && request.getNumArgs() != 5 ) {
					throw new InvalidRequestExistsException("wrong number of arguments");
				}
				
				if ( !topicCfg.isConsumer( userCfg.getPassword() )) {
					throw new InvalidRequestExistsException("no authority");
				}
				
				// 轮询分区消费
				if (request.getNumArgs() == 2) {
					partition = topicCfg.getRunningInfo().getPartitionByConsumer();
					offset = KafkaOffsetService.INSTANCE().getOffset(userCfg.getPassword(), topicCfg, partition.getPartition());
					
				// 指定分区消费
				} else {
					int pt = Integer.parseInt(new String(request.getArgs()[2]));
					partition = topicCfg.getRunningInfo().getPartition(pt);
					offset = Long.parseLong(new String(request.getArgs()[3]));
					if (request.getNumArgs() == 5) {
						maxBytes = Integer.parseInt(new String(request.getArgs()[4]));
					}
				} 
				
			}
			break;
		case CommandParse.PARTITIONS_CMD: 
			{
				if (request.getNumArgs() != 2) {
					throw new InvalidRequestExistsException("wrong number of arguments");
				}
	
				if (!topicCfg.isConsumer(userCfg.getPassword())) {
					throw new InvalidRequestExistsException("no authority");
				}
				
				return new RouteResult(RedisRequestType.KAFKA, requests, null);
			}
		case CommandParse.PRIVATE_CMD: 
			{
				// 全部内部代码调用 省掉验证部分
				return new RouteResult(RedisRequestType.KAFKA, requests, null);
			}
			
		case CommandParse.OFFSET_CMD: 
			{
				if (request.getNumArgs() != 4) {
					throw new InvalidRequestExistsException("wrong number of arguments");
				}
				
				if (!topicCfg.isConsumer(userCfg.getPassword())) {
					throw new InvalidRequestExistsException("no authority");
				}
				
				int pt = Integer.parseInt(new String(request.getArgs()[2]));
				partition = topicCfg.getRunningInfo().getPartition(pt);
				
				break;
			}
		}

		if ( partition == null ) {
			throw new InvalidRequestExistsException("wrong partition");
		}
		
		//
		KafkaPool pool = (KafkaPool) RedisEngineCtx.INSTANCE().getPoolMap().get( topicCfg.getPoolId() );
		PhysicalNode physicalNode = pool.getPhysicalNode( partition.getLeader().getId() );
		if (physicalNode == null)
			throw new PhysicalNodeUnavailableException("node unavailable.");
		
		KafkaRouteNode node = new KafkaRouteNode();
		node.setPhysicalNode(physicalNode);
		node.addRequestIndex(0);
		node.setOffset(offset);
		node.setPartition(partition.getPartition());
		node.setMaxBytes(maxBytes);

		List<RouteNode> nodes = new ArrayList<RouteNode>(1);
		nodes.add(node);

		RouteResult routeResult = new RouteResult(RedisRequestType.KAFKA, requests, nodes);
		return routeResult;
	}
}
