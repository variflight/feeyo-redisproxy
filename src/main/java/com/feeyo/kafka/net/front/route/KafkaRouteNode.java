package com.feeyo.kafka.net.front.route;

import com.feeyo.kafka.config.MetaDataOffset;
import com.feeyo.redis.net.front.route.RouteNode;

public class KafkaRouteNode extends RouteNode {
	
	private MetaDataOffset metaDataOffset;
	
	public MetaDataOffset getMetaDataOffset() {
		return metaDataOffset;
	}

	public void setMetaDataOffset(MetaDataOffset metaDataOffset) {
		this.metaDataOffset = metaDataOffset;
	}

}
