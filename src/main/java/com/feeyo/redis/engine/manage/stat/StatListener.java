package com.feeyo.redis.engine.manage.stat;


public interface StatListener {
	
	public void onBigKey(String password, String cmd, String key, int requestSize, int responseSize );
	public void onCollectionKey(String password, String cmd, String key, int requestSize );
	
	public void onScheduleToZore();
	public void onSchedulePeroid(int peroid);

}
