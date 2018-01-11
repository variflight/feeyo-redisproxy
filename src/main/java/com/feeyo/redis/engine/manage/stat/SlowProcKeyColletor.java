package com.feeyo.redis.engine.manage.stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SlowProcKeyColletor implements StatCollector {
	
	private static final long SLOW_COST = 50L; //指令处理时间
	
	private static final int LIMIT_MEM_LENGTH = 1000; //内存中存储key的限制
	
	private static final int LIMIT_SHOW_LENGTH = 100; //后端指令显示限制key个数
	
	private static float WEED_OUT_RADIO =  0.8F;	//淘汰比率
	
	private List<SlowProcKeyInfo> slowKeys = new ArrayList<SlowProcKeyInfo>(LIMIT_MEM_LENGTH);
	
	private SlowKeyDescComparator comparator = new SlowKeyDescComparator();
	
	private AtomicBoolean isBlocking = new AtomicBoolean(false);
	private AtomicBoolean isSorting = new AtomicBoolean(false);
	
	
	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills,
			boolean isCommandOnly) {
		
		if(procTimeMills < SLOW_COST)
			return;
		
		SlowProcKeyInfo slowKey = new SlowProcKeyInfo(cmd, key);
		procSlowKey(slowKey);
		
	}
	
	private void procSlowKey(SlowProcKeyInfo slowKey) {
		
		if ( isSorting.get() || !isBlocking.compareAndSet(false, true) ) {
			return;
		}
		try {
			int index = slowKeys.indexOf(slowKey);
			if(index >= 0) {
				slowKeys.get(index).count++;
			}else {
				if(isUpperLimit()) 
					slimSlowKey();
	
				slowKeys.add(slowKey);
			}
		}finally {
			isBlocking.set(false);
		}
		
	}

	private boolean isUpperLimit() {
		return slowKeys.size() >= LIMIT_MEM_LENGTH;
	}
	
	private void slimSlowKey() {
		sortSlowKey();
		while(slowKeys.size() >= LIMIT_MEM_LENGTH * WEED_OUT_RADIO)
			slowKeys.remove(slowKeys.size()-1);
	}
	
	private void sortSlowKey() {
		try {
			while(!isSorting .compareAndSet(false, true)){
		    }
			Collections.sort(slowKeys, comparator);
		}finally {
			isSorting.set(false);
		}
		
	}
	
	@Override
	public void onScheduleToZore() {
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	public List<SlowProcKeyInfo> getSlowKeyShowList(){
		sortSlowKey();
		return slowKeys.subList(0, LIMIT_SHOW_LENGTH);
	}
	
	public class SlowProcKeyInfo {
		public String lastCmd;
		public long count;
		public String key;
		
		public SlowProcKeyInfo(String lastCmd, String key) {
			this.lastCmd = lastCmd;
			this.key = key;
			this.count = 1;
		}

		@Override
		public boolean equals(Object obj) {
			if(obj == this)
				return true;
			if(obj == null || !(obj instanceof SlowProcKeyInfo))
				return false;
			SlowProcKeyInfo slowKey = (SlowProcKeyInfo) obj;
			return this.key.equals(slowKey.key);
		}
		
	}
	
	class SlowKeyDescComparator implements Comparator<SlowProcKeyInfo>{

		@Override
		public int compare(SlowProcKeyInfo slowKey1, SlowProcKeyInfo slowKey2) {
				if (slowKey1 == slowKey2)
					return 0;
				else if (slowKey1.count > slowKey2.count)
					return -1;
				else if (slowKey1.count == slowKey2.count) {
					if (null == slowKey1.key)
						return -1;
					return slowKey1.key.compareTo(slowKey2.key);
				}
				return 1;
		}
	}
	
}
