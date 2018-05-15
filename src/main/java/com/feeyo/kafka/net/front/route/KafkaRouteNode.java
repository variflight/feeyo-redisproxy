package com.feeyo.kafka.net.front.route;

import com.feeyo.kafka.net.backend.broker.BrokerPartitionOffset;
import com.feeyo.redis.net.front.route.RouteNode;

public class KafkaRouteNode extends RouteNode {
	
	private BrokerPartitionOffset partitionOffset;
	
	public BrokerPartitionOffset getPartitionOffset() {
		return partitionOffset;
	}

	public void setPartitionOffset(BrokerPartitionOffset partitionOffset) {
		this.partitionOffset = partitionOffset;
	}

}
