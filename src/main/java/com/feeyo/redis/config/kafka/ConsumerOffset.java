package com.feeyo.redis.config.kafka;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerOffset {
	private String consumer;
	private AtomicLong offset;
	private ConcurrentLinkedQueue<Long> defaultOffset;
	

	public ConsumerOffset(String consumer, long offset) {
		this.consumer = consumer;
		this.offset = new AtomicLong(offset);
		this.defaultOffset = new ConcurrentLinkedQueue<Long>();
	}

	public String getConsumer() {
		return consumer;
	}

	public long getOffset() {
		return offset.get();
	}
	
	public long poolOffset() {
		Long defaultOff = defaultOffset.poll();
		if ( defaultOff == null ) {
			return offset.getAndIncrement();
		}
		return defaultOff.longValue();
	}
	
	public void offerOffset(Long offset) {
		this.defaultOffset.offer(offset);
	}

	public void setConsumer(String consumer) {
		this.consumer = consumer;
	}

	public ConcurrentLinkedQueue<Long> getDefaultOffset() {
		return defaultOffset;
	}

	public void setDefaultOffset(ConcurrentLinkedQueue<Long> defaultOffset) {
		this.defaultOffset = defaultOffset;
	}
}
