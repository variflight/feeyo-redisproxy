package com.feeyo.kafka.net.front.route;

import com.feeyo.kafka.net.backend.metadata.DataOffset;
import com.feeyo.redis.net.front.route.RouteNode;

public class KafkaRouteNode extends RouteNode {
	
	private DataOffset _dataOffset;
	
	public DataOffset getDataOffset() {
		return _dataOffset;
	}

	public void setDataOffset(DataOffset dataOffset) {
		this._dataOffset = dataOffset;
	}

}
