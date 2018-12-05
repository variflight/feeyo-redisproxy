package com.feeyo.kafka.net.front.route;

import com.feeyo.redis.net.front.route.RouteNode;

public class KafkaRouteNode extends RouteNode {
	
	// 点位
	private long offset;
	
	// 分区
	private int partition;
	
	// 如果是消费的话，消费的bytes
	private int maxBytes;
	
	public long getOffset() {
		return offset;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getMaxBytes() {
		return maxBytes;
	}

	public void setMaxBytes(int maxBytes) {
		this.maxBytes = maxBytes;
	}
	
}
