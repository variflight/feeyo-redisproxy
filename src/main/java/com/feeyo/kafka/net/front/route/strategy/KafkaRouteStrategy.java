package com.feeyo.kafka.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.kafka.config.KafkaPoolCfg;
import com.feeyo.kafka.config.TopicCfg;
import com.feeyo.kafka.net.backend.broker.BrokerPartition;
import com.feeyo.kafka.net.backend.broker.offset.BrokerOffsetService;
import com.feeyo.kafka.net.backend.pool.KafkaPool;
import com.feeyo.kafka.net.front.route.KafkaRouteNode;
import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.net.codec.redis.RedisRequestType;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.route.InvalidRequestException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteNode;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.strategy.AbstractRouteStrategy;

/**
 * Kafka RouteStrategy
 *
 * @author yangtao
 */
public class KafkaRouteStrategy extends AbstractRouteStrategy {

	@Override
	public RouteResult route(UserCfg userCfg, List<RedisRequest> requests)
			throws InvalidRequestException, PhysicalNodeUnavailableException {

		RedisRequest request = requests.get(0);
		if (request.getNumArgs() < 2) {
			throw new InvalidRequestException("wrong number of arguments", false);
		}

		// 获取 topic
		String topicName = new String(request.getArgs()[1]);
		KafkaPoolCfg kafkaPoolCfg = (KafkaPoolCfg) RedisEngineCtx.INSTANCE().getPoolCfgMap().get( userCfg.getPoolId() );
		if (kafkaPoolCfg == null) {
			throw new InvalidRequestException("kafka pool not exists", false);
		}

		TopicCfg topicCfg = kafkaPoolCfg.getTopicCfgMap().get(topicName);
		if (topicCfg == null) {
			throw new InvalidRequestException("topic not exists", false);
		}

		if (topicCfg.getRunningInfo() == null) {
			throw new InvalidRequestException("topic not create or not load to kafka...", false);
		}

		// 分区
		BrokerPartition brokerPartition = null;
		long offset = -1;
		int maxBytes = 1;

		// 参数有效性校验
		switch (request.getPolicy().getHandleType()) {
		
			case CommandParse.PRODUCE_CMD: {
					if (request.getNumArgs() != 3 && request.getNumArgs() != 4) {
						throw new InvalidRequestException("wrong number of arguments", false);
					}
		
					if (!topicCfg.isProducer(userCfg.getPassword())) {
						throw new InvalidRequestException("no authority", false);
					}
		
					// 轮询分区
					if (request.getNumArgs() == 3) {
						brokerPartition = topicCfg.getRunningInfo().getPartitionByProducer();
		
					// 指定分区
					} else {
						int pt = Integer.parseInt(new String(request.getArgs()[2]));
						brokerPartition = topicCfg.getRunningInfo().getPartition(pt);
					}
				}
	
				break;
			case CommandParse.CONSUMER_CMD: {
					if (request.getNumArgs() != 2 && request.getNumArgs() != 4 && request.getNumArgs() != 5) {
						throw new InvalidRequestException("wrong number of arguments", false);
					}
		
					if (!topicCfg.isConsumer(userCfg.getPassword())) {
						throw new InvalidRequestException("no authority", false);
					}
		
					// 轮询分区
					if (request.getNumArgs() == 2) {
						brokerPartition = topicCfg.getRunningInfo().getPartitionByConsumer();
						offset = BrokerOffsetService.INSTANCE().getOffset(userCfg.getPassword(), topicCfg,
								brokerPartition.getPartition());
		
					// 指定分区
					} else {
						int pt = Integer.parseInt(new String(request.getArgs()[2]));
						brokerPartition = topicCfg.getRunningInfo().getPartition(pt);
						offset = Long.parseLong(new String(request.getArgs()[3]));
						if (request.getNumArgs() == 5) {
							maxBytes = Integer.parseInt(new String(request.getArgs()[4]));
						}
					}
		
				}
				break;
			case CommandParse.PARTITIONS_CMD: {
					if (request.getNumArgs() != 2) {
						throw new InvalidRequestException("wrong number of arguments", false);
					}
		
					if (!topicCfg.isConsumer(userCfg.getPassword())) {
						throw new InvalidRequestException("no authority", false);
					}
		
					return new RouteResult(RedisRequestType.KAFKA, requests, null);
				}
			
			case CommandParse.PRIVATE_CMD: {
					// 全部内部代码调用 省掉验证部分
					return new RouteResult(RedisRequestType.KAFKA, requests, null);
				}
	
			case CommandParse.OFFSET_CMD: {
					if (request.getNumArgs() != 4) {
						throw new InvalidRequestException("wrong number of arguments", false);
					}
		
					if (!topicCfg.isConsumer(userCfg.getPassword())) {
						throw new InvalidRequestException("no authority", false);
					}
		
					int pt = Integer.parseInt(new String(request.getArgs()[2]));
					brokerPartition = topicCfg.getRunningInfo().getPartition(pt);
				}
				break;
		}

		if (brokerPartition == null) {
			throw new InvalidRequestException("wrong partition", false);
		}

		//
		KafkaPool pool = (KafkaPool) RedisEngineCtx.INSTANCE().getPoolMap().get(topicCfg.getPoolId());
		PhysicalNode physicalNode = pool.getPhysicalNode(brokerPartition.getLeader().getId());
		if ( physicalNode == null ) {
			throw new PhysicalNodeUnavailableException("node unavailable.");
		//	
		} 

		// 注入 offset & partition & maxBytes
		//
		KafkaRouteNode node = new KafkaRouteNode();
		node.setPhysicalNode(physicalNode);
		node.addRequestIndex(0);
		node.setOffset(offset);
		node.setPartition(brokerPartition.getPartition());
		node.setMaxBytes(maxBytes);

		List<RouteNode> nodes = new ArrayList<RouteNode>(1);
		nodes.add(node);

		RouteResult routeResult = new RouteResult(RedisRequestType.KAFKA, requests, nodes);
		return routeResult;
	}
}
