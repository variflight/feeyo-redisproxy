package com.feeyo.redis.engine.manage.stat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

// collect user flow 
//
public class UserFlowCollector implements StatCollector {
	
	private static ConcurrentHashMap<String, UserFlow> userFlowMap = new ConcurrentHashMap<String, UserFlow>();

	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills,
			int waitTimeMills, boolean isCommandOnly, boolean isBypass) {
		if (isCommandOnly) {
			return;
		}

		
		UserFlow flow = userFlowMap.get(password);
		if ( flow == null ) {
			flow = new UserFlow();
			flow.password = password;
			userFlowMap.put(password, flow);
		}
		flow.netIn.addAndGet( requestSize );
		flow.netOut.addAndGet( responseSize );
	}

	@Override
	public void onScheduleToZore() {
		userFlowMap.clear();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	public ConcurrentHashMap<String, UserFlow> getUserFlowMap() {
		return userFlowMap;
	}
	
	//
	public static class UserFlow {
		public String password;
		public AtomicLong netIn = new AtomicLong(0);
		public AtomicLong netOut = new AtomicLong(0);
	}

}
