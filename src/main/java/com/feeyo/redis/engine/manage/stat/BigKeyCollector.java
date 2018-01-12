package com.feeyo.redis.engine.manage.stat;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.feeyo.redis.nio.util.TimeUtil;

public class BigKeyCollector implements StatCollector {
	
	public static int LENGTH = 500;
	public static final int BIGKEY_SIZE = 1024 * 256;  				// 大于 256K
	
	
	private List<BigKey> keys = new ArrayList<BigKey>( LENGTH );
	
	
	private AtomicBoolean locking = new AtomicBoolean(false);
	
	
	public List<BigKey> getBigkeys() {
		try {
			while (!locking.compareAndSet(false, true)) {
			}
			
			// 处理排序, 降序
			Collections.sort(keys, new BigKeyKeyComparator( false ));
			
			int len = keys.size() > 100 ? 100 : keys.size();
			List<BigKey> newList = new ArrayList<BigKey>( len );
			for(int i = 0; i < len; i++) {
				newList.add( keys.get(i) );
			}
			return newList;
			
		} finally {
			locking.set(false);
		}
	}

	
	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, 
			int procTimeMills, boolean isCommandOnly ) {
		
		// 大key 校验
		if (  requestSize < BIGKEY_SIZE && responseSize < BIGKEY_SIZE  ) {	
			return;
		}
		
		
		if ( !locking.compareAndSet(false, true) ) {
			return;
		}
		
		//
		try {
			if ( keys.size() == LENGTH ) {
				// 处理排序, 降序
				Collections.sort(keys, new BigKeyKeyComparator( false ));
				
				// 缩容
				while (keys.size() >= ( LENGTH * 0.5 ) ) {
					int index = keys.size() - 1;
					keys.remove( index );
				}
			}
			
			BigKey newBigKey = new BigKey();
			newBigKey.key = new String(key);
			
			
			int index = keys.indexOf( newBigKey );
			if (index >= 0) {
				BigKey oldBigKey = keys.get(index);
				oldBigKey.cmd = cmd;
				oldBigKey.size = requestSize > responseSize ? requestSize : responseSize;
				oldBigKey.lastUseTime = TimeUtil.currentTimeMillis();
				oldBigKey.count.incrementAndGet();
				
			} else {
				newBigKey.cmd = cmd;
				newBigKey.size = requestSize > responseSize ? requestSize : responseSize;
				newBigKey.lastUseTime = TimeUtil.currentTimeMillis();
				keys.add( newBigKey );
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
			
			keys.clear();
			
		} finally {
			locking.set(false);
		}
	}


	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	
	
	// sort
	public class BigKeyKeyComparator implements Comparator<BigKey> {

		boolean isASC;
		
		public BigKeyKeyComparator(boolean isASC) {
			this.isASC = isASC;
		}
		
		@Override
		public int compare(BigKey o1, BigKey o2) {
			
			if ( o1 == null || o2 == null ) {
				return -1;
			}
			
			long a, b;
            if ( isASC ) {
                a = o1.count.get();
                b = o2.count.get();
            } else {
                a = o2.count.get();
                b = o1.count.get();
            }
			
            if (a > b)
                return 1;	// 大于
            else if (a == b)
                return 0;	//等于
            else
                return -1;	//小于
		}
		
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
}