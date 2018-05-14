package com.feeyo.kafka.net.backend.runtime;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Metadata {
	
	private final String name;
	private final boolean internal;
	
	// 分区信息
	private final DataPartition[] partitions;
	private AtomicInteger producerIndex;
	private AtomicInteger consumerIndex;
	
	private final int partitionsCount;
	private Map<Integer, DataPartitionOffset> partitionOffsets;


	public Metadata(String name, boolean internal, DataPartition[] partitions) {
		this.name = name;
		this.internal = internal;
		this.partitions = partitions;
		this.partitionsCount = partitions.length;
		
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
		int index = getIndex(producerIndex);
		return this.partitions[index];
	}

	public DataPartition getConsumerDataPartition() {
		int index = getIndex(consumerIndex);
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

	private int getIndex(AtomicInteger ai) {
		for (;;) {
			int current = ai.get();
			int next = current + 1;
			next = next < partitionsCount ? next : 0;
			if (ai.compareAndSet(current, next))
				return next;
		}
	}

}