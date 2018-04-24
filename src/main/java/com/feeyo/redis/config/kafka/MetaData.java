package com.feeyo.redis.config.kafka;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaData {
	private final String name;
	private final boolean internal;
	// 分区信息
	private final MetaDataPartition[] partitions;
	private AtomicInteger producerIndex;
	private AtomicInteger consumerIndex;
	private final int partitionsCount;
	private Map<Integer, MetaDataOffset> offsets;

	public MetaData(String name, boolean internal, MetaDataPartition[] partitions) {
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

	public MetaDataPartition[] getPartitions() {
		return partitions;
	}

	public MetaDataPartition getProducerMetaDataPartition() {
		int index = getIndex(producerIndex);
		return this.partitions[index];
	}

	public MetaDataPartition getConsumerMetaDataPartition() {
		int index = getIndex(consumerIndex);
		return this.partitions[index];
	}

	public Map<Integer, MetaDataOffset> getOffsets() {
		return offsets;
	}
	
	public MetaDataOffset getMetaDataOffsetByPartition(int partition) {
		MetaDataOffset metaDataOffset = offsets.get(partition);
		if (metaDataOffset == null) {
			metaDataOffset = new MetaDataOffset(partition, 0);
			offsets.put(partition, metaDataOffset);
		}
		return metaDataOffset;
	}

	public void setOffsets(Map<Integer, MetaDataOffset> offsets) {
		this.offsets = offsets;
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
	
	public void close() {
		for (Entry<Integer, MetaDataOffset> entry : offsets.entrySet()) {
			entry.getValue().close();
		}
	}
	
	public void reset() {
		for (Entry<Integer, MetaDataOffset> entry : offsets.entrySet()) {
			entry.getValue().reset();
		}
	}
}
