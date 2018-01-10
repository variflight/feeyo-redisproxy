package com.feeyo.redis.engine.manage.stat;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.util.FileUtils;

public class NetFlowCollector extends AbstractStatCollector {
	

	private static final String USER_NET_FLOW_FILE_NAME = "user_net_flow_v";
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
		String date = getYesterdayDate();
		saveFile(date, false);
		userNetFlowMap.clear();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	public Set<Entry<String, UserNetFlow>> getUserFlowSet() {
		return userNetFlowMap.entrySet();
	}
	
	public static class UserNetFlow {
		public String password;
		public AtomicLong netIn = new AtomicLong(0);
		public AtomicLong netOut = new AtomicLong(0);
	}

	@Override
	protected void saveFile(String date, boolean isTemp) {
		if(userNetFlowMap.isEmpty())
			return;
		StringBuffer buffer = new StringBuffer();
		long totalNetIn = 0;
		long totalNetOut = 0;
		Set<Entry<String, UserNetFlow>> entrys = userNetFlowMap.entrySet();
		buffer.append("User").append(FIELD_SPARATOR).append("NetIn").append(FIELD_SPARATOR).append("NetOut").append(LINE_SPARATOR);
		for (Map.Entry<String, UserNetFlow> entry : entrys) { 
			if (!StatUtil.STAT_KEY.equals(entry.getKey())) {
				UserNetFlow userNetIo = entry.getValue();
				buffer.append(userNetIo.password).append(FIELD_SPARATOR).append(userNetIo.netIn.get()).append(FIELD_SPARATOR).append(userNetIo.netOut.get()).append(LINE_SPARATOR);
				totalNetIn = totalNetIn + userNetIo.netIn.get();
				totalNetOut = totalNetOut + userNetIo.netOut.get();
			}
		}
		buffer.append("total").append(FIELD_SPARATOR).append(totalNetIn).append(FIELD_SPARATOR).append(totalNetOut).append(LINE_SPARATOR);
		
		String filename = basepath+USER_NET_FLOW_FILE_NAME+date+FILE_TYPE;
		FileUtils.ensureCreateFile(filename, buffer.toString(), isTemp);
	}

}
