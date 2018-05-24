package com.feeyo.kafka.net.backend.broker;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.kafka.util.JsonUtils;

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
	
	public long getNewOffset() {
		Long defaultOff = oldOffsetQueue.poll();
		if ( defaultOff == null ) {
			return currentOffset.getAndIncrement();
		}
		return defaultOff.longValue();
	}
	
	public void returnOldOffset(Long offset) {
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
	
	
	public static void main(String[] args) {
		String s = "[1111111111111,2]";
		ConcurrentLinkedQueue<?> x  = JsonUtils.unmarshalFromString(s, ConcurrentLinkedQueue.class);
		Object obj = x.poll();
		while (obj != null) {
			
			if (obj instanceof Integer) {
				long ss = Long.parseLong(obj.toString());
				System.out.println(2);
				System.out.println(ss);
			} else if (obj instanceof Long) {
				long s1 = (long) obj;
				System.out.println(1);
				System.out.println(s1);
			}
			obj = x.poll();
		}
	}

}
