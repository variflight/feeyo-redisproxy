package com.feeyo.redis.engine.manage.stat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.feeyo.net.nio.util.TimeUtil;

// 
public class BigKeyCollector implements StatCollector {
	
	public static int LENGTH = 500;
	
	// required size
	public static int REQUIRED_SIZE = 1024 * 256;  				
	
	//
	//
	private ConcurrentHashMap<String, BigKey> bkHashMap = new ConcurrentHashMap<String, BigKey>();	// use search
	private List<BigKey> bkList = new ArrayList<BigKey>(); 											// use sort, 降序
	private AtomicBoolean locking = new AtomicBoolean(false);
	
	
	public void setSize(int size) {
		if ( size >= 1024 * 100 && size <= 1024 * 1024 * 2 )
			REQUIRED_SIZE = size;
	}
	
	public List<BigKey> getTop100() {
		
		try {
			
			while (!locking.compareAndSet(false, true)) {
			}
		
			int len = bkList.size() > 100 ? 100 : bkList.size();
			
			List<BigKey> newList = new ArrayList<BigKey>( len );
			for(int i = 0; i < len; i++) {
				newList.add( bkList.get(i) );
			}
			return newList;
			
		} finally {
			locking.set(false);
		}
	}
	
	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, 
			int procTimeMills, int waitTimeMills, boolean isCommandOnly) {
		
		if ( isCommandOnly) 
			return;
		
		//  check size
		if ( requestSize < REQUIRED_SIZE && responseSize < REQUIRED_SIZE  ) {	
			return;
		}
		
		//
		if ( !locking.compareAndSet(false, true) ) {
			return;
		}
		
		//
		try {
			if ( bkList.size() >= LENGTH ) {
				
				// 缩容
				while (bkList.size() >= ( LENGTH * 0.5 ) ) {
					int index = bkList.size() - 1;
					BigKey bk = bkList.remove( index );
					if ( bk != null ) {
						bkHashMap.remove( bk.key );
					}
				}
			}
			
			//
			BigKey newBK = new BigKey();
			newBK.key = key;
			
			int index = bkList.indexOf( newBK );
			if (index >= 0) {
				
				BigKey oldBK = bkHashMap.get(key);
				
				// 通过deleteResponseBigkey删掉的key
				if (oldBK == null) {
					oldBK = newBK;
					bkHashMap.put( key, oldBK );
				}
				oldBK.lastCmd = cmd;
				oldBK.size = requestSize > responseSize ? requestSize : responseSize;
				oldBK.lastUseTime = TimeUtil.currentTimeMillis();
				oldBK.count.incrementAndGet();
				oldBK.fromReq = requestSize >= REQUIRED_SIZE;
				oldBK.fromResp = responseSize >= REQUIRED_SIZE;
				
			} else {
				newBK.lastCmd = cmd;
				newBK.size = requestSize > responseSize ? requestSize : responseSize;
				newBK.lastUseTime = TimeUtil.currentTimeMillis();
				newBK.fromReq = requestSize >= REQUIRED_SIZE;
				newBK.fromResp = responseSize >= REQUIRED_SIZE;
				bkList.add( newBK );
				bkHashMap.put(key, newBK );
			}
			
		} finally {
			locking.set(false);
		}
	}
	
	@Override
	public void onScheduleToZore() {
		//
		try {
			while (!locking.compareAndSet(false, true)) {
			}
			
			bkList.clear();
			bkHashMap.clear();
			
		} finally {
			locking.set(false);
		}
		
	}

	@Override
	public void onSchedulePeroid(int peroid) {
		// ignore
	}
	
	
	public boolean isResponseBigkey(String cmd, String key) {
		
		/*
			后续需要对 指令类别做精细化控制
			如： HGETALL key  是慢查询， 但  HGET key field 则不是的情况
		 */
		
		BigKey bk = bkHashMap.get(key);
		if (bk != null && bk.fromResp && bk.lastCmd.equals(cmd) ) {
			return true;
		}
		return false;
	}
	
	public void deleteResponseBigkey(String key) {
		
		bkHashMap.remove(key);
		
//		BigKey bk = bkHashMap.get(key);
//		if (bk != null && bk.fromReq) {
//			bk.fromResp = false;
//		} else {
//			bkHashMap.remove(key);
//		}
	}
	
	//
	// 
	public static class BigKey implements Comparator<BigKey> {
		
		public static boolean isASC = false;
		
		public String lastCmd;
		public String key;
		public int size;
		public AtomicInteger count = new AtomicInteger(1);
		public boolean fromReq;	
		public boolean fromResp;	
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

}