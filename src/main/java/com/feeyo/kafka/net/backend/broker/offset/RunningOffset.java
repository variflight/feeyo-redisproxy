package com.feeyo.kafka.net.backend.broker.offset;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.feeyo.kafka.net.backend.broker.BrokerPartition;
import com.feeyo.kafka.net.backend.broker.BrokerPartitionOffset;

/*
 * Topic 运行时点位
 */
public class RunningOffset {
	
	private final String name;
	private final boolean internal;
	
	// 分区信息
	private final BrokerPartition[] partitions;
	private AtomicInteger producerIndex;
	private AtomicInteger consumerIndex;
	
	private final int partitionNum;
	private Map<Integer, BrokerPartitionOffset> partitionOffsets;


	public RunningOffset(String name, boolean internal, BrokerPartition[] partitions) {
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

	public BrokerPartition[] getBrokerPartitions() {
		return partitions;
	}

	public BrokerPartition getProducerBrokerPartition() {
		int index = getPartitionIndex(producerIndex);
		return this.partitions[index];
	}

	public BrokerPartition getConsumerBrokerPartition() {
		int index = getPartitionIndex( consumerIndex );
		return this.partitions[index];
	}
	
	public BrokerPartition getConsumerBrokerPartition(int partition) {
		for (BrokerPartition p : partitions) {
			if (p.getPartition() == partition) {
				return p;
			}
		}
		return null;
	}

	public Map<Integer, BrokerPartitionOffset> getPartitionOffsets() {
		return partitionOffsets;
	}
	
	public BrokerPartitionOffset getPartitionOffset(int partition) {
		BrokerPartitionOffset offset = partitionOffsets.get(partition);
		if (offset == null) {
			offset = new BrokerPartitionOffset(partition, 0, 0);
			partitionOffsets.put(partition, offset);
		}
		return offset;
	}

	public void setPartitionOffsets(Map<Integer, BrokerPartitionOffset> offsets) {
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