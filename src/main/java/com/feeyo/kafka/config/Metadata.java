package com.feeyo.kafka.config;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.protocol.ApiKeys;

import com.feeyo.kafka.codec.ApiVersionsResponse.ApiVersion;

public class Metadata {
	
	private final String name;
	private final boolean internal;
	
	// 分区信息
	private final DataPartition[] partitions;
	private AtomicInteger producerIndex;
	private AtomicInteger consumerIndex;
	
	private final int partitionsCount;
	private Map<Integer, DataOffset> offsets;
	private static Map<Short, ApiVersion> apiVersions = null;

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

	public Map<Integer, DataOffset> getDataOffsets() {
		return offsets;
	}
	
	public DataOffset getDataOffsetByPartition(int partition) {
		DataOffset dataOffset = offsets.get(partition);
		if (dataOffset == null) {
			dataOffset = new DataOffset(partition, 0, 0);
			offsets.put(partition, dataOffset);
		}
		return dataOffset;
	}

	public void setDataOffsets(Map<Integer, DataOffset> offsets) {
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

	
	public static void setApiVersions(Map<Short, ApiVersion> apiVersions) {
		Metadata.apiVersions = apiVersions;
	}
	
	public static ApiVersion getApiVersion(short key) {
		return Metadata.apiVersions.get(key);
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
