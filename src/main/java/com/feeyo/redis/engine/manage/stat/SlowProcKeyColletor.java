package com.feeyo.redis.engine.manage.stat;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SlowProcKeyColletor implements StatCollector {
	
	private static final long SLOW_COST = 50L; //指令处理时间
	
	private static final int LIMIT_MEM_LENGTH = 1000; //内存中存储key的限制
	
	private static final int LIMIT_SHOW_LENGTH = 100; //后端指令显示限制key个数
	
	private static final int LIMIT_SHOW_LEVEL = 5; //后端显示的key最小的count
	
	private static float WEED_OUT_RADIO =  0.5F;	//淘汰均值的比率 (根据规则变化)
	
	private static long KEY_RECENTLY_USE_TIME = 5 * 60 * 1000;	//
	
	//LRU-2
	private static ConcurrentHashMap<String, SlowProcKeyInfo> slowKeyShowMap = new ConcurrentHashMap<String, SlowProcKeyInfo>(LIMIT_SHOW_LENGTH);
	private static ConcurrentHashMap<String, SlowProcKeyInfo> slowKeyMemMap = new ConcurrentHashMap<String, SlowProcKeyInfo>(LIMIT_MEM_LENGTH);
	
	private static Map<String, SlowProcKeyInfo> descSortedMap;	//降序map -> 用于后端显示
	private static Map<String, SlowProcKeyInfo> ascSortedMap;	//升序map -> 用于淘汰数据
	
	public SlowProcKeyColletor() {
		descSortedMap = Collections.synchronizedMap(new TreeMap<String, SlowProcKeyInfo>(new DescComparator()));
		ascSortedMap = Collections.synchronizedMap(new TreeMap<String, SlowProcKeyInfo>(new AscComparator()));
	}
	
	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills,
			boolean isCommandOnly) {
		
		if(procTimeMills < SLOW_COST)
			return;
		
		cacheKey(cmd, key);
		
	}
	
	@Override
	public void onScheduleToZore() {
		slowKeyMemMap.clear();
		slowKeyShowMap.clear();
		descSortedMap.clear();
		ascSortedMap.clear();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
		//将memMap中count > LIMIT_LEVEL的key添加到showMap
		Iterator<Entry<String, SlowProcKeyInfo>> iter = slowKeyMemMap.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, SlowProcKeyInfo> entry = iter.next();
			SlowProcKeyInfo slowKey = entry.getValue();
			if(slowKey.count.get() >= LIMIT_SHOW_LEVEL) {
				putMemKey2Show(slowKey);
				iter.remove();
			}
		}
		
		slimSlowKeyMap(slowKeyShowMap, LIMIT_SHOW_LENGTH);
	}
	
	private void cacheKey(String cmd, String key) {
		
		if(slowKeyMemMap.containsKey(key)) {
			SlowProcKeyInfo slowKeyInfo = slowKeyMemMap.get(key);
			slowKeyInfo.count.incrementAndGet();
			slowKeyInfo.lastCmd = cmd;
			slowKeyInfo.lastUseTime = System.currentTimeMillis();
		}else {
			slimSlowKeyMap(slowKeyMemMap, LIMIT_MEM_LENGTH);
			
			SlowProcKeyInfo slowKeyInfo = new SlowProcKeyInfo(cmd, key);
			slowKeyMemMap.put(key, slowKeyInfo);
		}
	}
	
	private void putMemKey2Show(SlowProcKeyInfo slowKey) {
		if(slowKeyShowMap.containsKey(slowKey.key)) {
			SlowProcKeyInfo slowKeyInfo = slowKeyMemMap.get(slowKey.key);
			slowKeyInfo.count.addAndGet(slowKey.count.get());
			slowKeyInfo.lastCmd = slowKey.lastCmd;
			slowKeyInfo.lastUseTime = slowKey.lastUseTime;
		}else {
			slowKeyShowMap.put(slowKey.key, slowKey);
		}
	}
	
	private void slimSlowKeyMap(Map<String, SlowProcKeyInfo> slowKeyMap, int limitLength) {
		
		if(slowKeyMap.size() <= limitLength) 
			return;
		//第一轮淘汰掉5min 没用 & 较小的count的key
		int preSize = slowKeyMap.size();
		long current = System.currentTimeMillis() ;
		//count阀值计算
		long thresholdValue = calculateThresholdCount(slowKeyMap);
		Iterator<Entry<String, SlowProcKeyInfo>> iter = slowKeyMap.entrySet().iterator();
		while(iter.hasNext()) {
			if(slowKeyMap.size() <= limitLength) 
				return;
			Entry<String, SlowProcKeyInfo> entry = iter.next();
			SlowProcKeyInfo slowKey = entry.getValue();
			if(slowKey.count.get() < thresholdValue && current - slowKey.lastUseTime > KEY_RECENTLY_USE_TIME) {
				iter.remove();
			}
		}
		
		//执行到此处说明slowKeyMap.size()>limitLength->修改规则
		if(preSize - slowKeyMap.size() < (int)(limitLength * 0.1)) {
			KEY_RECENTLY_USE_TIME = KEY_RECENTLY_USE_TIME > 1 * 60 * 1000L ?  KEY_RECENTLY_USE_TIME - 30* 1000 : 2 * 60 * 1000L;
			WEED_OUT_RADIO = WEED_OUT_RADIO < 0.75F ? WEED_OUT_RADIO + 0.025F : 0.75F;
		}
		
		//淘汰count较小的值，保证size <= limitLength
		Map<String, SlowProcKeyInfo> dscSlowKeyMap = getAscSlowKeyMap(slowKeyMap);
		for(String key : dscSlowKeyMap.keySet()) {
			if(slowKeyMap.size() <= limitLength) 
				return;
			slowKeyMap.remove(key);
		}
		
	}
	
	private long calculateThresholdCount(Map<String, SlowProcKeyInfo> slowKeyMap) {
		return (long) (calculateAvgCount(slowKeyMap) * WEED_OUT_RADIO) ;
	}
	
	private long calculateAvgCount(Map<String, SlowProcKeyInfo> slowKeyMap) {
		long count_sum = 0;
		for(SlowProcKeyInfo slowKey : slowKeyMap.values()) {
			count_sum += slowKey.count.get();
		}
		return count_sum / slowKeyMap.size();
	}
	
	public class SlowProcKeyInfo {
		public String lastCmd;
		public AtomicLong count;
		public String key;
		public long lastUseTime;
		
		public SlowProcKeyInfo(String lastCmd, String key) {
			this.lastCmd = lastCmd;
			this.key = key;
			this.count = new AtomicLong(1);
			this.lastUseTime = System.currentTimeMillis();
		}
	}

	
	//降序显示
	public Set<Entry<String, SlowProcKeyInfo>> getSlowKeyShowSet() {
		descSortedMap.clear();
		descSortedMap.putAll(slowKeyShowMap);
		return descSortedMap.entrySet();
	}
	
	public Map<String, SlowProcKeyInfo> getAscSlowKeyMap(Map<String, SlowProcKeyInfo> slowKeyMap ){
		ascSortedMap.clear();
		ascSortedMap.putAll(slowKeyMap);
		return ascSortedMap;
	}

	class DescComparator implements Comparator<String>{
		@Override
		public int compare(String key1, String key2) {
			SlowProcKeyInfo slowKey1 = slowKeyShowMap.get(key1);
			SlowProcKeyInfo slowKey2 = slowKeyShowMap.get(key2);
			if (slowKey1 == slowKey2)
				return 0;
			else if (slowKey1.count.get() > slowKey2.count.get())
				return -1;
			else if (slowKey1.count.get() == slowKey2.count.get()) {
				if (null == slowKey1.key)
					return -1;
				return slowKey1.key.compareTo(slowKey2.key);
			}
			return 1;
		}
	}
	
	class AscComparator implements Comparator<String>{

		@Override
		public int compare(String key1, String key2) {
			SlowProcKeyInfo slowKey1 = slowKeyShowMap.get(key1);
			SlowProcKeyInfo slowKey2 = slowKeyShowMap.get(key2);
			if (slowKey1 == slowKey2)
				return 0;
			else if (slowKey1.count.get() > slowKey2.count.get())
				return 1;
			else if (slowKey1.count.get() == slowKey2.count.get()) {
				if (null == slowKey1.key)
					return 1;
				return slowKey2.key.compareTo(slowKey1.key);
			}
			return -1;
		}
		
	}
	
}
