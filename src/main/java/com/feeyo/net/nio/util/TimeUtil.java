package com.feeyo.net.nio.util;

import java.util.Calendar;

/**
 * 弱精度的计时器，考虑性能不使用同步策略。
 **/
public class TimeUtil {

	private static volatile long CURRENT_TIME = System.currentTimeMillis();
	private static volatile int CURRENT_SECOND = (int) ((CURRENT_TIME / 1000) % 60);
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
	
	public static final int currentSecond() {
		return CURRENT_SECOND;
	}

	public static final long currentTimeNanos() {
		return System.nanoTime();
	}

	public static final void update() {
		CURRENT_TIME = System.currentTimeMillis();
		CURRENT_SECOND = (int) ((CURRENT_TIME / 1000) % 60);
	}

	
	public static String formatTimestamp(long mills) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis( mills );

		int hour = cal.get( Calendar.HOUR_OF_DAY );
		int minute = cal.get( Calendar.MINUTE );
		int second = cal.get( Calendar.SECOND );
		int millsecond = cal.get( Calendar.MILLISECOND );
		
		StringBuffer sb = new StringBuffer();
		sb.append( hour ).append(":");
		
		if ( minute >= 10)	
			sb.append( minute).append(":");
		else 
			sb.append("0").append( minute).append(":");
		
		if ( second >= 10 ) 
			sb.append( second );
		else
			sb.append("0").append( second );
		sb.append(".").append( millsecond );
		
		return sb.toString();
	}
	
}