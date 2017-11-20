package com.feeyo.redis.engine.manage.stat;

import com.feeyo.redis.engine.codec.RedisRequestPolicy;

public interface StatListener {
	
	public void onBigKey(String password, String cmd, String key, int requestSize, int responseSize );
	
	public void onWatchType(String password, RedisRequestPolicy policy, String key, int requestSize);
	
	public void onScheduleToZore();
	public void onSchedulePeroid(int peroid);

}
