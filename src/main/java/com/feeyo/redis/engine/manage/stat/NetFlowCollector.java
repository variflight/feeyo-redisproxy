package com.feeyo.redis.engine.manage.stat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class NetFlowCollector implements StatCollector {
	
	private static ConcurrentHashMap<String, UserNetFlow> userNetFlowMap = new ConcurrentHashMap<String, UserNetFlow>();

	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills,
			boolean isCommandOnly) {
		
		UserNetFlow userNetFlow = userNetFlowMap.get(password);
		if ( userNetFlow == null ) {
			userNetFlow = new UserNetFlow();
			userNetFlow.password = password;
			userNetFlowMap.put(password, userNetFlow);
		}
		userNetFlow.netIn.addAndGet(requestSize);
		userNetFlow.netOut.addAndGet(responseSize);
	}

	@Override
	public void onScheduleToZore() {
		userNetFlowMap.clear();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	public ConcurrentHashMap<String, UserNetFlow> getUserFlowMap() {
		return userNetFlowMap;
	}
	
	public static class UserNetFlow {
		public String password;
		public AtomicLong netIn = new AtomicLong(0);
		public AtomicLong netOut = new AtomicLong(0);
	}

}
