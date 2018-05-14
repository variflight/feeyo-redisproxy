package com.feeyo.kafka.net.front.route;

import com.feeyo.kafka.net.backend.metadata.DataPartitionOffset;
import com.feeyo.redis.net.front.route.RouteNode;

public class KafkaRouteNode extends RouteNode {
	
	private DataPartitionOffset partitionOffset;
	
	public DataPartitionOffset getPartitionOffset() {
		return partitionOffset;
	}

	public void setPartitionOffset(DataPartitionOffset partitionOffset) {
		this.partitionOffset = partitionOffset;
	}

}
