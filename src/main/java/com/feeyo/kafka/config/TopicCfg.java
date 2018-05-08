package com.feeyo.kafka.config;

import java.util.Collections;
import java.util.HashSet;

public class TopicCfg {
	
	private final int poolId;
	
	private final String name;
	private final int partitions;
	private final short replicationFactor;
	
	private final HashSet<String> producers = new HashSet<String>(); 
	private final HashSet<String> consumers = new HashSet<String>(); 
	
	private Metadata metadata;
	
	public TopicCfg(String name, int poolId, int partitions, short replicationFactor, 
			String[] producerArr, String[] consumerArr) {
		
		this.poolId = poolId;
		
		this.name = name;
		this.partitions = partitions;
		this.replicationFactor = replicationFactor;
		
		Collections.addAll(producers, producerArr);
		Collections.addAll(consumers, consumerArr);
	}

	public String getName() {
		return name;
	}

	public int getPoolId() {
		return poolId;
	}

	public int getPartitions() {
		return partitions;
	}

	public short getReplicationFactor() {
		return replicationFactor;
	}

	public HashSet<String> getProducers() {
		return producers;
	}

	public HashSet<String> getConsumers() {
		return consumers;
	}
	
	public boolean isProducer(String producer) {
		return producers.contains(producer);
	}
	
	public boolean isConsumer(String consumer) {
		return consumers.contains(consumer);
	}
	
	public Metadata getMetadata() {
		return metadata;
	}

	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
	}
	
}
