package com.feeyo.kafka.net.backend.runtime;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TopicRunningInfo {
	
	private final String name;
	private final boolean internal;
	
	// 分区信息
	private final DataPartition[] partitions;
	private AtomicInteger producerIndex;
	private AtomicInteger consumerIndex;
	
	private final int partitionNum;
	private Map<Integer, DataPartitionOffset> partitionOffsets;


	public TopicRunningInfo(String name, boolean internal, DataPartition[] partitions) {
		this.name = name;
		this.internal = internal;
		this.partitions = partitions;
		this.partitionNum = partitions.length;
		
		this.producerIndex = new AtomicInteger(0);
		this.consumerIndex = new AtomicInteger(0);
	}

	public String getName() {
		return name;
	}

	public boolean isInternal() {
		return internal;
	}

	public DataPartition[] getPartitions() {
		return partitions;
	}

	public DataPartition getProducerDataPartition() {
		int index = getPartitionIndex(producerIndex);
		return this.partitions[index];
	}

	public DataPartition getConsumerDataPartition() {
		int index = getPartitionIndex( consumerIndex );
		return this.partitions[index];
	}
	
	public DataPartition getConsumerDataPartition(int partition) {
		for (DataPartition p : partitions) {
			if (p.getPartition() == partition) {
				return p;
			}
		}
		return null;
	}

	public Map<Integer, DataPartitionOffset> getPartitionOffsets() {
		return partitionOffsets;
	}
	
	public DataPartitionOffset getPartitionOffset(int partition) {
		DataPartitionOffset offset = partitionOffsets.get(partition);
		if (offset == null) {
			offset = new DataPartitionOffset(partition, 0, 0);
			partitionOffsets.put(partition, offset);
		}
		return offset;
	}

	public void setPartitionOffsets(Map<Integer, DataPartitionOffset> offsets) {
		this.partitionOffsets = offsets;
	}

	private int getPartitionIndex(AtomicInteger index) {
		for (;;) {
			int current = index.get();
			int next = current + 1;
			next = next < partitionNum ? next : 0;
			if (index.compareAndSet(current, next))
				return next;
		}
	}

}