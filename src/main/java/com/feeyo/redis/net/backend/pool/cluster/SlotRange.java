package com.feeyo.redis.net.backend.pool.cluster;

/**
 * Slot
 */
public class SlotRange {
	
	private int start;
	private int end;
	
	public SlotRange(int start, int end) {
		this.start = start;
		this.end = end;
	}

	public int getStart() {
		return start;
	}

	public int getEnd() {
		return end;
	}
	
	@Override
	public String toString() {
		return "start=" + start + ", end=" + end;
	}		
}
