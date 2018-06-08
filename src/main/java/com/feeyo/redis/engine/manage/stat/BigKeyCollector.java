package com.feeyo.redis.engine.manage.stat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.feeyo.redis.nio.util.TimeUtil;

public class BigKeyCollector implements StatCollector {
	
	public static int LENGTH = 500;
	
	// required size
	public static int REQUIRED_SIZE = 1024 * 256;  				// 大于 256K
	
	private ConcurrentHashMap<String, BigKey> keyMap = new ConcurrentHashMap<String, BigKey>();
	
	private AtomicBoolean locking = new AtomicBoolean(false);
	
	public void setSize(int size) {
		if ( size >= 1024 * 256 && size <= 1024 * 1024 * 2 )
			REQUIRED_SIZE = size;
	}
	
	public List<BigKey> getBigkeys() {
		
		try {
			
			while (!locking.compareAndSet(false, true)) {
			}
			
			// 处理排序, 降序
			TreeMap<String, BigKey> sortedKeyMap = sortBigKeyMap(keyMap, false);
			
			int len = sortedKeyMap.size() > 100 ? 100 : sortedKeyMap.size();
			List<BigKey> newList = new ArrayList<BigKey>( len );
			
			Iterator<BigKey> iter = sortedKeyMap.values().iterator();
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
		
		// 大key 校验
		if (  requestSize < REQUIRED_SIZE && responseSize < REQUIRED_SIZE  ) {	
			return;
		}
		
		if ( !locking.compareAndSet(false, true) ) {
			return;
		}
		
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
				oldBigKey.size = requestSize > responseSize ? requestSize : responseSize;
				oldBigKey.lastUseTime = TimeUtil.currentTimeMillis();
				oldBigKey.count.incrementAndGet();
				oldBigKey.isReqHolder = requestSize >= REQUIRED_SIZE;
				oldBigKey.isRespHolder = responseSize >= REQUIRED_SIZE;
				
			} else {
				newBigKey.cmd = cmd;
				newBigKey.size = requestSize > responseSize ? requestSize : responseSize;
				newBigKey.lastUseTime = TimeUtil.currentTimeMillis();
				newBigKey.isReqHolder = requestSize >= REQUIRED_SIZE;
				newBigKey.isRespHolder = responseSize >= REQUIRED_SIZE;
				keyMap.put(key, newBigKey );
			}
			
		} finally {
			locking.set(false);
		}
	}
	
	@Override
	public void onScheduleToZore() {
		try {
			while (!locking.compareAndSet(false, true)) {
			}
			keyMap.clear();
			
		} finally {
			locking.set(false);
		}
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	//sort
	private TreeMap<String, BigKey> sortBigKeyMap(Map<String, BigKey> keyMap, boolean isAsc) {
		TreeMap<String, BigKey> sortedKeyMap = new TreeMap<String, BigKey>(new BigKeyKeyComparator(keyMap, isAsc));
		sortedKeyMap.putAll(keyMap);
		return sortedKeyMap;
	}
	
	public boolean isResponseBigkey(String cmd, String key) {
		BigKey bk = keyMap.get(key);
		if (bk != null && bk.cmd.equals(cmd) && bk.isRespHolder) {
			return true;
		}
		return false;
	}
	
	public void delResponseBigkey(String key) {
		
		BigKey bk = keyMap.get(key);
		if (bk != null && bk.isReqHolder) {
			bk.isRespHolder = false;
		} else {
			keyMap.remove(key);
		}
	}
	
	public static class BigKey {
		public String cmd;
		public String key;
		public int size;
		public AtomicInteger count = new AtomicInteger(1);
		public long lastUseTime;
		public boolean isReqHolder;	
		public boolean isRespHolder;	
		
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