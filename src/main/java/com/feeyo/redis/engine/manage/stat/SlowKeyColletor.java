package com.feeyo.redis.engine.manage.stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SlowKeyColletor implements StatCollector {
	
	private static int LENGTH = 5000;
	
	private List<SlowKey> keys = new ArrayList<SlowKey>( LENGTH );
	
	private AtomicBoolean locking = new AtomicBoolean(false);
	
	@Override
	public void onCollect(String host, String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills,
                          int waitTimeMills, boolean isCommandOnly, boolean isBypass) {

        if (isCommandOnly) {
            return;
        }
		
		// 小于 50毫秒不记录
		if( procTimeMills < 50 )
			return;
		
		if ( !locking.compareAndSet(false, true) ) {
			return;
		}
		
		try {
			
			if ( keys.size() == LENGTH ) {
				
				// 处理排序, 降序
				Collections.sort(keys, new SlowKeyComparator( false ));
				
				// 缩容
				while (keys.size() >= ( LENGTH * 0.5 ) ) {
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
	
	public List<SlowKey> getSlowKeys() {
		try {
			while (!locking.compareAndSet(false, true)) {
			}
			
			// 处理排序, 降序
			Collections.sort(keys, new SlowKeyComparator( false ));
			
			return keys.subList(0, keys.size() > 100 ? 100 : keys.size());
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
	public void onSchedulePeroid(int peroid) {}
	

	// sort
	public class SlowKeyComparator implements Comparator<SlowKey> {

		boolean isASC;
		
		public SlowKeyComparator(boolean isASC) {
			this.isASC = isASC;
		}
		
		@Override
		public int compare(SlowKey o1, SlowKey o2) {
			
			if ( o1 == null || o2 == null ) {
				return -1;
			}
			
			long a, b;
            if ( isASC ) {
                a = o1.count;
                b = o2.count;
            } else {
                a = o2.count;
                b = o1.count;
            }
			
            if (a > b)
                return 1;	// 大于
            else if (a == b)
                return 0;	//等于
            else
                return -1;	//小于
		}
		
	}

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
