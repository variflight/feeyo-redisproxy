package com.feeyo.kafka.config;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.protocol.ApiKeys;

import com.feeyo.kafka.codec.ApiVersionsResponse.ApiVersion;

public class MetaData {
	
	private final String name;
	private final boolean internal;
	
	// 分区信息
	private final MetaDataPartition[] partitions;
	private AtomicInteger producerIndex;
	private AtomicInteger consumerIndex;
	private final int partitionsCount;
	private Map<Integer, MetaDataOffset> offsets;
	private static Map<Short, ApiVersion> apiVersions = null;

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
	
	public static void setApiVersions(Map<Short, ApiVersion> apiVersions) {
		MetaData.apiVersions = apiVersions;
	}
	
	public static ApiVersion getApiVersion(short key) {
		return MetaData.apiVersions.get(key);
	}
	
	public static short getProduceVersion() {
		// 现在代码最多支持到5
		short version = 5;
		ApiVersion apiVersion = apiVersions.get(ApiKeys.PRODUCE.id);
		if (apiVersion.maxVersion < version){
			version =  apiVersion.maxVersion;
		} else if (apiVersion.minVersion > version) {
			version = -1;
		}
		
		return version;
	}
	
	public static short getConsumerVersion() {
		// 现在代码最多支持到7
		short version = 7;
		ApiVersion apiVersion = apiVersions.get(ApiKeys.FETCH.id);
		if (apiVersion.maxVersion < version) {
			version = apiVersion.maxVersion;
		} else if (apiVersion.minVersion > version) {
			version = -1;
		}
		
		return version;
	}
}
