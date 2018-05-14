package com.feeyo.kafka.net.backend.metadata;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.fastjson.annotation.JSONField;

public class ConsumerOffset {
	
	private String consumer;
	private AtomicLong currentOffset;
	private ConcurrentLinkedQueue<Long> oldOffsetQueue;
	

	public ConsumerOffset(String consumer, long offset) {
		this.consumer = consumer;
		this.currentOffset = new AtomicLong(offset);
		this.oldOffsetQueue = new ConcurrentLinkedQueue<Long>();
	}
	
	public ConsumerOffset() {
		this(null, 0);
	}

	public void setConsumer(String consumer) {
		this.consumer = consumer;
	}
	
	public String getConsumer() {
		return consumer;
	}

	public long getCurrentOffset() {
		return currentOffset.get();
	}
	
	/**
	 * offset设置成kafka的logstartoffset
	 * @param update
	 */
	public void setOffsetToLogStartOffset(long update) {
		
		while (true) {
            long current = currentOffset.get();
            if (current >= update) {
            		break;
            }
            if (currentOffset.compareAndSet(current, update))
                break;
        }
		
	}
	
	@JSONField(serialize=false)
	public long getNewOffset() {
		Long defaultOff = oldOffsetQueue.poll();
		if ( defaultOff == null ) {
			return currentOffset.getAndIncrement();
		}
		return defaultOff.longValue();
	}
	
	public void revertOldOffset(Long offset) {
		this.oldOffsetQueue.offer(offset);
	}

	public ConcurrentLinkedQueue<Long> getOldOffsetQueue() {
		return oldOffsetQueue;
	}

	public void setOldOffsetQueue(ConcurrentLinkedQueue<Long> oldOffsetQueue) {
		this.oldOffsetQueue = oldOffsetQueue;
	}

	public void setCurrentOffset(long currentOffset) {
		this.currentOffset.set(currentOffset);
	}

	
}
