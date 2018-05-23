package com.feeyo.kafka.net.backend.broker;

public class BrokerPartition {
	
	private final int partition;
	
	private final BrokerNode leader;
	private final BrokerNode[] replicas;

	public BrokerPartition(int partition, BrokerNode leader, BrokerNode[] replicas) {
		this.partition = partition;
		this.leader = leader;
		this.replicas = replicas;
	}

	public int getPartition() {
		return partition;
	}

	public BrokerNode getLeader() {
		return leader;
	}

	public BrokerNode[] getReplicas() {
		return replicas;
	}
}
