package com.feeyo.redis.engine.manage.stat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BigKeyCollector implements StatCollector {
	
	private static final int BIGKEY_SIZE = 1024 * 256;  				// 大于 256K
	
	private static BigKeyDelegation delegator = new BigKeyDelegation();
	
	public static class BigKey {
		public String cmd;
		public String key;
		public int size;
		public AtomicInteger count = new AtomicInteger(1);
		public long lastUseTime;
	}
	
	@Override
	public void onScheduleToZore() {
		delegator.doAtZero();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, 
			int procTimeMills, boolean isCommandOnly ) {
		
		// 大key 校验
		if (  requestSize < BIGKEY_SIZE && responseSize < BIGKEY_SIZE  ) {	
			return;
		}
		delegator.doOnCollect(cmd, key, requestSize, responseSize);
		
	}
	
	public ConcurrentHashMap<String, BigKey> getBigkeyMap() {
		return delegator.getBigkeyMap();
	}
	
}
