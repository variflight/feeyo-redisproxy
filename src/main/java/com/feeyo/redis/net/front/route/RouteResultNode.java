package com.feeyo.redis.net.front.route;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.config.kafka.MetaDataOffset;
import com.feeyo.redis.net.backend.pool.PhysicalNode;


public class RouteResultNode {
	
	private PhysicalNode physicalNode;
	private List<Integer> requestIndexs;
	private MetaDataOffset kafkaMetaDataOffset;
	
	public RouteResultNode() {
		requestIndexs = new ArrayList<Integer>();
	}

	public PhysicalNode getPhysicalNode() {
		return physicalNode;
	}

	public void setPhysicalNode(PhysicalNode physicalNode) {
		this.physicalNode = physicalNode;
	}

	public List<Integer> getRequestIndexs() {
		return requestIndexs;
	}

	public void addRequestIndex(int index) {
		requestIndexs.add(index);
	}

	public MetaDataOffset getKafkaMetaDataOffset() {
		return kafkaMetaDataOffset;
	}

	public void setKafkaMetaDataOffset(MetaDataOffset kafkaMetaDataOffset) {
		this.kafkaMetaDataOffset = kafkaMetaDataOffset;
	}

}
