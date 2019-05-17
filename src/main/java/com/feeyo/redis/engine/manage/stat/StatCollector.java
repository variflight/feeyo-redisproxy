package com.feeyo.redis.engine.manage.stat;


public interface StatCollector {

    public void onCollect(String password, String cmd, String key, int requestSize, int responseSize,
                          int procTimeMills, int waitTimeMills, boolean isCommandOnly, boolean isBypass);
	
	public void onScheduleToZore();
	public void onSchedulePeroid(int peroid);

}
