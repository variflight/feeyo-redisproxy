package com.feeyo.kafka.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.annotation.JSONField;

public class MetaDataOffset {
	
	private final int partition;
	private volatile long producerOffset;
	private Map<String, ConsumerOffset> offsets;
	private volatile boolean isClosed = false;
	
	public MetaDataOffset (int partition, long producerOffset) {
		this.producerOffset = producerOffset;
		this.offsets = new ConcurrentHashMap<>();
		this.partition = partition;
	}
	
	public long getProducerOffset() {
		return producerOffset;
	}

	public void setProducerOffset(long producerOffset) {
		this.producerOffset = producerOffset;
	}

	public Map<String, ConsumerOffset> getOffsets() {
		return offsets;
	}

	public void setOffsets(Map<String, ConsumerOffset> offsets) {
		this.offsets = offsets;
	}
	
	public int getPartition() {
		return partition;
	}
	
	public long getConsumerOffset(String consumer) {
		if (isClosed) {
			return -1L;
		}
		ConsumerOffset consumerOffset = getConsumerOffsetByConsumer(consumer);
		return consumerOffset.poolOffset();
	}
	
	@JSONField(serialize=false)
	public List<String> getAllConsumerOffset() {
		List<String> list = new ArrayList<>();
		for (ConsumerOffset consumerOffset : offsets.values()) {
			StringBuffer sb = new StringBuffer();
			sb.append(consumerOffset.getConsumer()).append(":").append(consumerOffset.getOffset());
			list.add(sb.toString());
		}
		return list;
	}
	
	public void sendDefaultConsumerOffsetBack(long offset, String consumer) {
		if (offset < 0) {
			return;
		}
		ConsumerOffset consumerOffset = getConsumerOffsetByConsumer(consumer);
		consumerOffset.offerOffset(offset);
	}
	
	private ConsumerOffset getConsumerOffsetByConsumer(String consumer) {
		ConsumerOffset consumerOffset = offsets.get(consumer);
		if (consumerOffset == null) {
			consumerOffset = new ConsumerOffset(consumer, 0);
			offsets.put(consumer, consumerOffset);
		}
		return consumerOffset;
	}
	
	public void close() {
		this.isClosed = true;
	}
	
	public void reset() {
		this.isClosed = false;
	}
}
