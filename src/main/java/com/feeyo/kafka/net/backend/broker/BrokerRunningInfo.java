package com.feeyo.kafka.net.backend.broker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * Broker 中 topic 的运行信息
 */
public class BrokerRunningInfo {
	
	// topic
	private final String name;
	private final boolean internal;
	private final int partitionNum;
	private ConcurrentHashMap<Integer, BrokerPartition> partitions = new ConcurrentHashMap<>();
	

	private AtomicInteger producerIndex;
	private AtomicInteger consumerIndex;
	
	public BrokerRunningInfo(String name, boolean internal, BrokerPartition[] partitions) {
		this.name = name;
		this.internal = internal;
		
		this.partitionNum = partitions.length;
		for(BrokerPartition p: partitions) {
			this.partitions.put(p.getPartition(), p);
		}
		
		this.producerIndex = new AtomicInteger(0);
		this.consumerIndex = new AtomicInteger(0);
	}

	public String getName() {
		return name;
	}

	public boolean isInternal() {
		return internal;
	}

	public ConcurrentHashMap<Integer, BrokerPartition> getPartitions() {
		return partitions;
	}

	public BrokerPartition getPartitionByProducer() {
		int index = getLoopPartitionIndex(producerIndex);
		return this.partitions.get( index );
	}

	public BrokerPartition getPartitionByConsumer() {
		int index = getLoopPartitionIndex( consumerIndex );
		return this.partitions.get( index );
	}
	
	public BrokerPartition getPartition(int partition) {
		return this.partitions.get( partition );
	}

	private int getLoopPartitionIndex(AtomicInteger index) {
		for (;;) {
			int current = index.get();
			int next = current + 1;
			next = next < partitionNum ? next : 0;
			if (index.compareAndSet(current, next))
				return next;
		}
	}

}