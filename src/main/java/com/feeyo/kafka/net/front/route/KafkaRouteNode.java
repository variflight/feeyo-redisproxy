package com.feeyo.kafka.net.front.route;

import com.feeyo.kafka.config.DataOffset;
import com.feeyo.redis.net.front.route.RouteNode;

public class KafkaRouteNode extends RouteNode {
	
	private DataOffset _dataOffset;
	
	public DataOffset getDataOffset() {
		return _dataOffset;
	}

	public void setDataOffset(DataOffset metaDataOffset) {
		this._dataOffset = metaDataOffset;
	}

}
