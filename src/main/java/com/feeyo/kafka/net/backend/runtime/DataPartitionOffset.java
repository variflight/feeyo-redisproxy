package com.feeyo.kafka.net.backend.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.annotation.JSONField;

public class DataPartitionOffset {
	
	private int partition;
	private volatile long producerOffset;
	private volatile long logStartOffset;
	
	@JSONField(serialize=false)
	private Map<String, ConsumerOffset> consumerOffsets;
	
	public DataPartitionOffset (int partition, long producerOffset, long logStartOffset) {
		this.producerOffset = producerOffset;
		this.consumerOffsets = new ConcurrentHashMap<>();
		this.partition = partition;
		this.logStartOffset = logStartOffset;
	}
	
	public DataPartitionOffset() {
		this(0, 0, 0);
	}
	
	public long getProducerOffset() {
		return producerOffset;
	}

	public void setOffsets(long producerOffset, long logStartOffset) {
		this.producerOffset = producerOffset;
		this.logStartOffset = logStartOffset;
	}

	public Map<String, ConsumerOffset> getConsumerOffsets() {
		return consumerOffsets;
	}

	public void setConsumerOffsets(Map<String, ConsumerOffset> offsets) {
		this.consumerOffsets = offsets;
	}
	
	public int getPartition() {
		return partition;
	}
	

	
	@JSONField(serialize=false)
	public List<String> getAllConsumerOffset() {
		List<String> list = new ArrayList<>();
		for (ConsumerOffset consumerOffset : consumerOffsets.values()) {
			StringBuffer sb = new StringBuffer();
			sb.append(consumerOffset.getConsumer()).append(":").append(consumerOffset.getCurrentOffset());
			list.add(sb.toString());
		}
		return list;
	}
	
	public void revertConsumerOffset(String consumer, long offset) {
		
		if (offset < 0) {
			return;
		}
		
		// 点位超出范围两种可能。
		// 1:日志被kafka自动清除，
		// 2:消费快过生产。
		if ( offset < logStartOffset ) {
			// 如果是日志被kafka自动清除的点位超出范围，把点位设置成kafka日志开始的点位
			ConsumerOffset consumerOffset = getConsumerOffsetByConsumer(consumer);
			consumerOffset.setOffsetToLogStartOffset(logStartOffset);
		} else {
			ConsumerOffset consumerOffset = getConsumerOffsetByConsumer(consumer);
			consumerOffset.revertOldOffset(offset);
		}

	}
	
	public ConsumerOffset getConsumerOffsetByConsumer(String consumer) {
		ConsumerOffset consumerOffset = consumerOffsets.get(consumer);
		if (consumerOffset == null) {
			consumerOffset = new ConsumerOffset(consumer, 0);
			consumerOffsets.put(consumer, consumerOffset);
		}
		return consumerOffset;
	}

	public long getLogStartOffset() {
		return logStartOffset;
	}

	public void setLogStartOffset(long logStartOffset) {
		this.logStartOffset = logStartOffset;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public void setProducerOffset(long producerOffset) {
		this.producerOffset = producerOffset;
	}
	
}