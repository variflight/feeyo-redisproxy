package com.feeyo.kafka.config;

public class DataPartition {
	
	private final int partition;
	private final DataNode leader;
	private final DataNode[] replicas;

	public DataPartition(int partition, DataNode leader, DataNode[] replicas) {
		this.partition = partition;
		this.leader = leader;
		this.replicas = replicas;
	}

	public int getPartition() {
		return partition;
	}

	public DataNode getLeader() {
		return leader;
	}

	public DataNode[] getReplicas() {
		return replicas;
	}
}
