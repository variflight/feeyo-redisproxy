package com.feeyo.kafka.net.backend.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerPartition {
	
	private final int partition;
	
	private final BrokerNode leader;
	private final BrokerNode[] replicas;
	
	// 生产者 & 消费者 的点位管理
	private volatile long producerOffset;
	private volatile long logStartOffset;
	
	private Map<String, ConsumerOffset> consumerOffsets = new ConcurrentHashMap<String, ConsumerOffset>();
	

	public BrokerPartition(int partition, BrokerNode leader, BrokerNode[] replicas) {
		this.partition = partition;
		this.leader = leader;
		this.replicas = replicas;
	}
	
	public int getPartition() {
		return partition;
	}

	public BrokerNode getLeader() {
		return leader;
	}

	public BrokerNode[] getReplicas() {
		return replicas;
	}
	

	public long getLogStartOffset() {
		return logStartOffset;
	}

	public void setLogStartOffset(long logStartOffset) {
		this.logStartOffset = logStartOffset;
	}
	
	public long getProducerOffset() {
		return producerOffset;
	}

	public void setProducerOffset(long producerOffset, long logStartOffset) {
		this.producerOffset = producerOffset;
		this.logStartOffset = logStartOffset;
	}
	
	public void setProducerOffset(long producerOffset) {
		this.producerOffset = producerOffset;
	}
	
	// consumer offset
	// ----------------------------------------------------------------------
	//
	public ConsumerOffset getConsumerOffset(String consumer) {
		ConsumerOffset consumerOffset = consumerOffsets.get(consumer);
		if (consumerOffset == null) {
			consumerOffset = new ConsumerOffset(consumer, 0);
			consumerOffsets.put(consumer, consumerOffset);
		}
		return consumerOffset;
	}
	
	public void returnConsumerOffset(String consumer, long offset) {
		
		if (offset < 0) {
			return;
		}
		
		// 点位超出范围两种可能。
		// 1:日志被kafka自动清除，
		// 2:消费快过生产。
		if ( offset < logStartOffset ) {
			// 如果是日志被kafka自动清除的点位超出范围，把点位设置成kafka日志开始的点位
			ConsumerOffset consumerOffset = getConsumerOffset(consumer);
			consumerOffset.setOffsetToLogStartOffset(logStartOffset);
			
		} else {
			ConsumerOffset consumerOffset = getConsumerOffset(consumer);
			consumerOffset.revertOldOffset(offset);
		}
	}
	
	//
	public void addConsumerOffset(ConsumerOffset consumerOffset) {
		consumerOffsets.put(consumerOffset.getConsumer(), consumerOffset);
	}
	
	public void removeConsumerOffset(String consumer) {
		consumerOffsets.remove(consumer);
	}
	
	public Map<String, ConsumerOffset> getConsumerOffsets() {
		return consumerOffsets;
	}

	public void setConsumerOffsets(Map<String, ConsumerOffset> consumerOffsets) {
		this.consumerOffsets = consumerOffsets;
	}
	
}
