package com.feeyo.redis.config.kafka;

import java.util.HashSet;

public class KafkaCfg {
	private final String topic;
	private final int poolId;
	private final int partitions;
	private final short replicationFactor;
	private final HashSet<String> producers = new HashSet<String>(); 
	private final HashSet<String> consumers = new HashSet<String>(); 
	private MetaData metaData;
	
	public KafkaCfg(String topic, int poolId, int partitions, short replicationFactor, String[] producers, String[] consumers) {
		this.topic = topic;
		this.poolId = poolId;
		this.partitions = partitions;
		this.replicationFactor = replicationFactor;
		for (String producer : producers) {
			this.producers.add(producer);
		}
		for (String consumer : consumers) {
			this.consumers.add(consumer);
		}
	}

	public String getTopic() {
		return topic;
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
	
	public MetaData getMetaData() {
		return metaData;
	}

	public void setMetaData(MetaData metaData) {
		this.metaData = metaData;
	}
	
}
