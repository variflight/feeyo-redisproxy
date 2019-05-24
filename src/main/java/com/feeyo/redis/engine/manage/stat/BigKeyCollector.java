package com.feeyo.redis.engine.manage.stat;

import com.feeyo.net.nio.util.TimeUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// 
public class BigKeyCollector implements StatCollector {
	public static int LENGTH = 500;

	// required size
	public static volatile int REQUIRED_SIZE = 1024 * 256;

	private ConcurrentHashMap<String, BigKey> bkHashMap = new ConcurrentHashMap<String, BigKey>(); // use
																									// search

	private AtomicLong bkSize = new AtomicLong(0);

	// bigkey 总数及旁路数
	private AtomicLong totalCount = new AtomicLong(0);
	private AtomicLong bypassCount = new AtomicLong(0);

	public void setSize(int size) {
		REQUIRED_SIZE = size;
	}

	//
	public List<BigKey> getTop100() {

		try {

			List<BigKey> newList = new ArrayList<>( 100 );

			Collection<BigKey> bigKeySet = bkHashMap.values();
			if (bigKeySet == null || bigKeySet.isEmpty()) {
				return newList;
			}

			// 排序
			List<BigKey> keyList = new ArrayList<>(bigKeySet);
			Collections.sort(keyList, new BigKeyComparator());
			if (keyList.size() <= 100) {
				return keyList;
			}

			newList = keyList.subList(0, 100);
			return newList;

		} finally {

		}

	}

	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills,
			int waitTimeMills, boolean isCommandOnly, boolean isBypass) {

		if (isCommandOnly)
			return;

		// check size
		if (requestSize < REQUIRED_SIZE && responseSize < REQUIRED_SIZE) {
			return;
		}

		if (isBypass) 
			bypassCount.incrementAndGet();
		//
		totalCount.incrementAndGet();

		// 判断是否存在 big key map
		BigKey bk = bkHashMap.get(key);
		if (bk == null) {
			// 不存在 判断是否超过 bk size
			if (bkSize.get() > LENGTH) {
				// 找到最小值 然后remove
				removeMinBigKey();
			}
			bk = new BigKey();
			bk.key = key;
			bk.lastCmd = cmd;
			bk.size = requestSize > responseSize ? requestSize : responseSize;
			bk.lastUseTime = TimeUtil.currentTimeMillis();
			bk.count.incrementAndGet();
			bk.fromReq = requestSize >= REQUIRED_SIZE;
			bk.fromResp = responseSize >= REQUIRED_SIZE;
			bkSize.incrementAndGet();
			// 添加
			bkHashMap.put(key, bk);
		} else {
			bk.key = key;
			bk.lastCmd = cmd;
			bk.size = requestSize > responseSize ? requestSize : responseSize;
			bk.lastUseTime = TimeUtil.currentTimeMillis();
			bk.count.incrementAndGet();
			bk.fromReq = requestSize >= REQUIRED_SIZE;
			bk.fromResp = responseSize >= REQUIRED_SIZE;
		}

	}

	@Override
	public void onScheduleToZore() {

		bkHashMap.clear();
		totalCount.set(0);
		bypassCount.set(0);
		bkSize.set(0);
	}

	@Override
	public void onSchedulePeroid(int peroid) {
		// ignore
	}

	public boolean isResponseBigkey(String cmd, String key) {

		/*
		 * 后续需要对 指令类别做精细化控制 如： HGETALL key 是慢查询， 但 HGET key field 则不是的情况
		 */

		BigKey bk = bkHashMap.get(key);
		if (bk != null && bk.fromResp && bk.lastCmd.equals(cmd)) {
			return true;
		}
		return false;
	}

	public void deleteResponseBigkey(String key) {
		bypassCount.incrementAndGet();
		totalCount.incrementAndGet();
		bkHashMap.remove(key);
		bkSize.decrementAndGet();
	}

	public long getBigKeyCount() {
		return totalCount.get();
	}

	public long getBypassBigKeyCount() {
		return bypassCount.get();
	}

	private void removeMinBigKey() {
		Collection<BigKey> bigKeySet = bkHashMap.values();
		if (bigKeySet == null || bigKeySet.isEmpty()) {
			return;
		}
		if (bkSize.get() < LENGTH) {
			return;
		}

		// 排序
		List<BigKey> keyList = new ArrayList<>(bigKeySet);
		Collections.sort(keyList, new BigKeyComparator());

		int size = keyList.size();
		BigKey minKey = keyList.get(size - 1);
		int minCount = minKey.count.get();
		for (int i = size - 1; i > 400; i--) {
			BigKey bk = keyList.get(i);
			if (bk != null && bk.count.get() == minCount) {
				bkSize.decrementAndGet();
				bkHashMap.remove(bk.key);
			}
		}
		keyList.clear();
	}

	
	public static class BigKeyComparator implements Comparator<BigKey> {
		
		public static boolean isASC = false;

		@Override
		public int compare(BigKey o1, BigKey o2) {

			if (o1 == null || o2 == null) {
				return -1;
			}

			long a, b;
			if (isASC) {
				a = o1.count.get();
				b = o2.count.get();
			} else {
				a = o2.count.get();
				b = o1.count.get();
			}

			if (a > b)
				return 1; // 大于
			else if (a == b)
				return 0; // 等于
			else
				return -1; // 小于
		}
		
	}
	
	//
	public static class BigKey extends BigKeyComparator {

		public String lastCmd;
		public String key;
		public int size;
		public AtomicInteger count = new AtomicInteger(1);
		public boolean fromReq;
		public boolean fromResp;
		public long lastUseTime;

		@Override
		public boolean equals(Object obj) {
			if (obj == this)
				return true;

			if (obj == null || !(obj instanceof BigKey))
				return false;

			BigKey bigkey = (BigKey) obj;
			return this.key.equals(bigkey.key);
		}
	}

}