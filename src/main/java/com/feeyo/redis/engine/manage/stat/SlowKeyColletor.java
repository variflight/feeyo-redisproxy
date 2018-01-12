package com.feeyo.redis.engine.manage.stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SlowKeyColletor implements StatCollector {
	
	private static int KEY_SIZE = 5000;
	private static int REDUECE_SIZE = (int)(KEY_SIZE * 0.5);
	
	private List<SlowKey> keys = new ArrayList<SlowKey>( KEY_SIZE );
	
	private AtomicBoolean locking = new AtomicBoolean(false);
	
	
	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills,
			boolean isCommandOnly) {
		
		// 小于 50毫秒不记录
		if( procTimeMills < 50 )
			return;
		
		if ( !locking.compareAndSet(false, true) ) {
			return;
		}
		
		try {
			
			if ( keys.size() == KEY_SIZE ) {
				
				sort();
				
				// 缩容
				while (keys.size() >= REDUECE_SIZE ) {
					int index = keys.size() - 1;
					keys.remove( index );
				}
			}

			SlowKey slowKey = new SlowKey(cmd, key);
			int index = keys.indexOf(slowKey);
			if (index >= 0) {
				keys.get(index).count++;
			} else {
				keys.add( slowKey );
			}

		} finally {
			locking.set(false);
		}
		
	}
	
	private void sort() {
		
		// 处理排序
		Collections.sort(keys, new Comparator<SlowKey>(){
			@Override
			public int compare(SlowKey k1, SlowKey k2) {
				if (k1 == k2)
					return 0;
				else if (k1.count > k2.count)
					return -1;
				else if (k1.count == k2.count) {
					if (null == k1.key)
						return -1;
					return k1.key.compareTo(k2.key);
				}
				return 1;
			}
		});
		
	}
	
	public List<SlowKey> getSlowKeys() {
		try {
			while (!locking.compareAndSet(false, true)) {
			}
			
			sort();
			
			return keys.subList(0, keys.size() > 100 ? 100 : keys.size());
		} finally {
			locking.set(false);
		}
	}
	
	@Override
	public void onScheduleToZore() {}

	@Override
	public void onSchedulePeroid(int peroid) {}
	

	public class SlowKey {
		
		public String cmd;
		public String key;
		public long count;
		
		public SlowKey(String cmd, String key) {
			this.cmd = cmd;
			this.key = key;
			this.count = 1;
		}

		@Override
		public boolean equals(Object obj) {
			if(obj == this)
				return true;
			
			if(obj == null || !(obj instanceof SlowKey))
				return false;
			
			SlowKey slowKey = (SlowKey) obj;
			return this.key.equals(slowKey.key);
		}
	}
	
}
