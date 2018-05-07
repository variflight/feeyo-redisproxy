package com.feeyo.kafka.net.front.route.strategy;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.kafka.config.KafkaCfg;
import com.feeyo.kafka.config.MetaDataPartition;
import com.feeyo.kafka.net.backend.pool.KafkaPool;
import com.feeyo.redis.config.UserCfg;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.codec.RedisRequest;
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
	
	
	//
	private KafkaCfg getKafkaCfg(String password, RedisRequest request, boolean isConsumer) throws InvalidRequestExistsException {
		
		String topic = new String(request.getArgs()[1]);
		
		KafkaCfg kafkaCfg = RedisEngineCtx.INSTANCE().getKafkaMap().get(topic);
		if (kafkaCfg == null) {
			throw new InvalidRequestExistsException("topic not exists");
		}
		
		if (!isConsumer && !kafkaCfg.isProducer( password )) {
			throw new InvalidRequestExistsException("no authority");
		}
		
		if (isConsumer && !kafkaCfg.isConsumer( password )) {
			throw new InvalidRequestExistsException("no authority");
		}
		
		if (kafkaCfg.getMetaData() == null) {
			throw new InvalidRequestExistsException("topic not create or not load to kafka...");
		} 
		
		return kafkaCfg;
	}
	
    @Override
    public RouteResult route(UserCfg userCfg, List<RedisRequest> requests) 
			throws InvalidRequestExistsException, PhysicalNodeUnavailableException {

		RedisRequest request = requests.get(0);

		KafkaCfg kafkaCfg;
		MetaDataPartition partition;
		if ( request.getPolicy().getHandleType() == CommandParse.PRODUCE_CMD) {
			if (request.getNumArgs() != 3) {
				throw new InvalidRequestExistsException("wrong number of arguments");
			}


			kafkaCfg = getKafkaCfg(userCfg.getPassword(), request, false);
			partition = kafkaCfg.getMetaData().getProducerMetaDataPartition();
			
		} else {
			
			if (request.getNumArgs() != 2 && request.getNumArgs() != 4 ) {
				throw new InvalidRequestExistsException("wrong number of arguments");
			}
			
			kafkaCfg = getKafkaCfg(userCfg.getPassword(), request, true);
			
			if (request.getNumArgs() == 4) {
				int pt = Integer.parseInt(new String(request.getArgs()[2]));
				partition = kafkaCfg.getMetaData().getConsumerMetaDataPartition(pt);
				if (partition == null) {
					throw new InvalidRequestExistsException("wrong partition");
				}
			} else {
				partition = kafkaCfg.getMetaData().getConsumerMetaDataPartition();
			}
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
