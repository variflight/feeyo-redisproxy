package com.feeyo.kafka.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.kafka.net.backend.pool.KafkaPool;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.config.kafka.KafkaCfg;
import com.feeyo.redis.config.kafka.MetaDataPartition;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisRequestPolicy;
import com.feeyo.redis.net.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.route.InvalidRequestExistsException;
import com.feeyo.redis.net.front.route.PhysicalNodeUnavailableException;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
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
		RedisRequestPolicy policy = request.getPolicy();

		KafkaCfg kafkaCfg;
		MetaDataPartition partition;
		if (policy.getHandleType() == CommandParse.PRODUCE_CMD) {
			if (request.getNumArgs() != 3) {
				throw new InvalidRequestExistsException("wrong number of arguments");
			}

			String topic = new String(request.getArgs()[1]);
			kafkaCfg = RedisEngineCtx.INSTANCE().getKafkaMap().get(topic);
			
			if (kafkaCfg == null) {
				throw new InvalidRequestExistsException("topic not exists");
			}
			
			if (!kafkaCfg.isProducer(userCfg.getPassword())) {
				throw new InvalidRequestExistsException("no authority");
			}
			
			if (kafkaCfg.getMetaData() == null) {
				throw new InvalidRequestExistsException("topic not create or not load to kafka...");
			} 
			
			partition = kafkaCfg.getMetaData().getProducerMetaDataPartition();
		} else {
			if (request.getNumArgs() != 2) {
				throw new InvalidRequestExistsException("wrong number of arguments");
			}

			String topic = new String(request.getArgs()[1]);
			kafkaCfg = RedisEngineCtx.INSTANCE().getKafkaMap().get(topic);
			if (kafkaCfg == null) {
				throw new InvalidRequestExistsException("topic not exists");
			}

			if (!kafkaCfg.isConsumer(userCfg.getPassword())) {
				throw new InvalidRequestExistsException("no authority");
			}
			
			if (kafkaCfg.getMetaData() == null) {
				throw new InvalidRequestExistsException("topic not create or not load to kafka...");
			} 
			
			partition = kafkaCfg.getMetaData().getConsumerMetaDataPartition();
		}

		List<RouteResultNode> nodes = new ArrayList<RouteResultNode>();
		KafkaPool pool = (KafkaPool) RedisEngineCtx.INSTANCE().getPoolMap().get(kafkaCfg.getPoolId());
		RouteResultNode node = new RouteResultNode();

		PhysicalNode physicalNode = pool.getPhysicalNode(partition.getLeader().getId());
		if (physicalNode == null)
			throw new PhysicalNodeUnavailableException("node unavailable.");
		node.setPhysicalNode(physicalNode);
		node.addRequestIndex(0);
		node.setKafkaMetaDataOffset( kafkaCfg.getMetaData().getMetaDataOffsetByPartition(partition.getPartition()) );

		nodes.add(node);

		RouteResult routeResult = new RouteResult(RedisRequestType.KAFKA, requests, nodes);
		return routeResult;
	}
}
