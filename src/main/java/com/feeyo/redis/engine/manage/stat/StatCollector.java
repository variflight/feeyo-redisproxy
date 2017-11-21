package com.feeyo.redis.engine.manage.stat;


public interface StatCollector {
	
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, 
			int procTimeMills, boolean isCommandOnly );
	
	public void onScheduleToZore(long zeroTimeMillis);
	public void onSchedulePeroid(int peroid);

}
