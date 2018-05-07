package com.feeyo.kafka.net.front.route;

import com.feeyo.kafka.config.DataOffset;
import com.feeyo.redis.net.front.route.RouteNode;

public class KafkaRouteNode extends RouteNode {
	
	private DataOffset metaDataOffset;
	
	public DataOffset getMetaDataOffset() {
		return metaDataOffset;
	}

	public void setMetaDataOffset(DataOffset metaDataOffset) {
		this.metaDataOffset = metaDataOffset;
	}

}
