package com.feeyo.redis.engine.manage.stat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.feeyo.redis.nio.util.TimeUtil;

public class BigKeyCollector implements StatCollector {
	
	public static int LENGTH = 500;
	public static int BIGKEY_SIZE = 1024 * 256;  				// 大于 256K
	
	private HashMap<String, BigKey> reqKeyMap = new HashMap<String, BigKey>();
	private HashMap<String, BigKey> respKeyMap = new HashMap<String, BigKey>();
	
	private AtomicBoolean locking = new AtomicBoolean(false);
	
	public List<BigKey> getBigkeys() {
		
		try {
			while (!locking.compareAndSet(false, true)) {
			}
			// 合并
			TreeMap<String, BigKey> mergeMap =  mapCombine(reqKeyMap, respKeyMap);
			
			// 处理排序, 降序
			mergeMap = sortBigKeyMap(mergeMap, false);
			
			int len = mergeMap.size() > 100 ? 100 : mergeMap.size();
			List<BigKey> newList = new ArrayList<BigKey>( len );
			
			Iterator<BigKey> iter = mergeMap.values().iterator();
			for(int i = 0; i < len; i++) {
				newList.add( iter.next() );
			}
			
			return newList;
			
		} finally {
			locking.set(false);
		}
	}
	
	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, 
			int procTimeMills, int waitTimeMills, boolean isCommandOnly ) {
		
		if (isCommandOnly) {
			return;
		}
		
		if(requestSize >= BIGKEY_SIZE) {
			dealKeyMap(reqKeyMap, cmd, key, requestSize);
		}
		
		if(responseSize >= BIGKEY_SIZE) {
			dealKeyMap(respKeyMap, cmd, key, responseSize);
		}
	}
	
	
	private void dealKeyMap(Map<String, BigKey> keyMap,  String cmd, String key, int size) {
		
		if ( !locking.compareAndSet(false, true) ) {
			return;
		}
		
		//
		try {
			if ( keyMap.size() == LENGTH ) {
				
				// 处理排序, 降序
				TreeMap<String, BigKey> sortKeyMap = sortBigKeyMap(keyMap, false);
				// 缩容
				while (keyMap.size() >= ( LENGTH * 0.5 ) ) {
					keyMap.remove(sortKeyMap.pollLastEntry().getKey());
				}
			}
			
			BigKey newBigKey = new BigKey();
			newBigKey.key = new String(key);
			
			if (keyMap.containsKey(key)) {
				BigKey oldBigKey = keyMap.get(key);
				oldBigKey.cmd = cmd;
				oldBigKey.size = size;
				oldBigKey.lastUseTime = TimeUtil.currentTimeMillis();
				oldBigKey.count.incrementAndGet();
				
			} else {
				newBigKey.cmd = cmd;
				newBigKey.size = size;
				newBigKey.lastUseTime = TimeUtil.currentTimeMillis();
				keyMap.put(key, newBigKey );
			}
			
		} finally {
			locking.set(false);
		}
		
	}
	
	private TreeMap<String, BigKey> mapCombine(HashMap<String, BigKey> m1, HashMap<String, BigKey>m2  ) {  
		
		TreeMap<String, BigKey> map = new TreeMap<String, BigKey>();
        List<HashMap<String, BigKey>> list = new ArrayList<HashMap<String, BigKey>>();
		list.add(m1);
		list.add(m2);
        for (HashMap<String, BigKey> m : list) {  
            Iterator<Entry<String, BigKey>> it = m.entrySet().iterator();  
            while (it.hasNext()) { 
            	Entry<String, BigKey> entry = it.next();
            	String key = entry.getKey();
            	BigKey value = entry.getValue();
                if (!map.containsKey(key)) {  
                    map.put(key, value);
                } else if(map.get(key).count.get() < value.count.get()){ 
                	 map.put(key, value);
                }  
            }  
        }  
        return map;  
    }

	@Override
	public void onScheduleToZore() {
		try {
			while (!locking.compareAndSet(false, true)) {
			}
			
			reqKeyMap.clear();
			respKeyMap.clear();
			
		} finally {
			locking.set(false);
		}
	}
	
	public void setBigkeySize(int size) {
		BIGKEY_SIZE = size;
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	//sort
	private TreeMap<String, BigKey> sortBigKeyMap(Map<String, BigKey> keyMap, boolean isAsc) {
		TreeMap<String, BigKey> sortMap = new TreeMap<String, BigKey>(new BigKeyKeyComparator(keyMap, isAsc));
		sortMap.putAll(keyMap);
		return sortMap;
	}
	
	// TODO 待完善
	public boolean isResponseBigkey(String key, String cmd) {
		return respKeyMap.containsKey(key);
	}
	
	public static class BigKey {
		public String cmd;
		public String key;
		public int size;
		public AtomicInteger count = new AtomicInteger(1);
		public long lastUseTime;
		
		@Override
		public boolean equals(Object obj) {
			if(obj == this)
				return true;
			
			if(obj == null || !(obj instanceof BigKey))
				return false;
			
			BigKey bigkey = (BigKey) obj;
			return this.key.equals(bigkey.key);
		}
	}
	
	class BigKeyKeyComparator implements Comparator<String> {
		
		private boolean isASC;
		private Map<String, BigKey> keyMap;
		
		public BigKeyKeyComparator(Map<String, BigKey> keyMap, boolean isASC) {
			this.keyMap = keyMap;
			this.isASC = isASC;
		}
		
		@Override
		public int compare(String o1, String o2) {
			
			if ( o1 == null || o2 == null || 
					keyMap.get(o1) == null || keyMap.get(o2) == null ) {
				return -1;
			}
			
			long a, b;
	        if ( isASC ) {
	            a = keyMap.get(o1).count.get();
	            b = keyMap.get(o2).count.get();
	        } else {
	            a = keyMap.get(o2).count.get();
	            b = keyMap.get(o1).count.get();
	        }
			
	        if (a > b)
	            return 1;	// 大于
	        else if (a == b)
	            return isASC ? o1.compareTo(o2) : o2.compareTo(o1);	//等于
	        else
	            return -1;	//小于
		}
		
	}
}