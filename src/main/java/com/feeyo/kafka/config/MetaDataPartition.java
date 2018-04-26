package com.feeyo.kafka.config;

public class MetaDataPartition {
	private final int partition;
	private final MetaDataNode leader;
	private final MetaDataNode[] replicas;

	public MetaDataPartition(int partition, MetaDataNode leader, MetaDataNode[] replicas) {
		this.partition = partition;
		this.leader = leader;
		this.replicas = replicas;
	}

	public int getPartition() {
		return partition;
	}

	public MetaDataNode getLeader() {
		return leader;
	}

	public MetaDataNode[] getReplicas() {
		return replicas;
	}
}
