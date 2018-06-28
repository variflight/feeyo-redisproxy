package com.feeyo.net.nio.util;

import java.util.Calendar;

/**
 * 弱精度的计时器，考虑性能不使用同步策略。
 **/
public class TimeUtil {

	private static volatile long CURRENT_TIME = System.currentTimeMillis();
	private static volatile int CURRENT_TIME_SECOND_INDEX = (int) ((CURRENT_TIME / 1000) % 60);
	
	private static volatile int OFFSET = 0;

	static {
		Calendar cal = Calendar.getInstance();
		int zoneOffset = cal.get(java.util.Calendar.ZONE_OFFSET);
		int dstOffset = cal.get(java.util.Calendar.DST_OFFSET);
		OFFSET = zoneOffset + dstOffset;
	}

	public static final long currentUtcTimeMillis() {
		return CURRENT_TIME - OFFSET;
	}

	public static final long currentTimeMillis() {
		return CURRENT_TIME;
	}
	
	public static final int currentTimeSecondIndex() {
		return CURRENT_TIME_SECOND_INDEX;
	}

	public static final long currentTimeNanos() {
		return System.nanoTime();
	}

	public static final void update() {
		CURRENT_TIME = System.currentTimeMillis();
		CURRENT_TIME_SECOND_INDEX = (int) ((CURRENT_TIME / 1000) % 60);
	}
	
}