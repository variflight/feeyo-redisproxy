package com.feeyo.redis.engine.manage.stat;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.nio.util.TimeUtil;

public class BigKey implements StatListener {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BigKey.class );
	
	private static ConcurrentHashMap<String, BigKey> bigkeyMap = new ConcurrentHashMap<String, BigKey>();
	
	public String cmd;
	public String key;
	public int size;
	public AtomicInteger count = new AtomicInteger(1);
	public long lastUseTime;
	
	
	public ConcurrentHashMap<String, BigKey> getBigkeyMap() {
		return bigkeyMap;
	}

	
	@Override
	public void onBigKey(String user, String cmd, String key, int requestSize, int responseSize) {
		
		if ( bigkeyMap.size() > 500 ) {
			for (Entry<String, BigKey> entry : bigkeyMap.entrySet()) {
				BigKey bigKey = entry.getValue();
				// TODO 后序优化
				// 清除： 最近5分钟没有使用过 && 使用总次数小于5 && 小于1M
				if (TimeUtil.currentTimeMillis() - bigKey.lastUseTime > 5 * 60 * 1000
						&& bigKey.count.get() < 5 && bigKey.size < 1 * 1024 * 1024) {
					bigkeyMap.remove(entry.getKey());
				}
			}
			LOGGER.info("bigkey clear. after clear bigkey length is :" + bigkeyMap.size());
		}
			
		String keyStr = new String(key);
		BigKey bigkey = bigkeyMap.get( keyStr );
		if ( bigkey == null ) {
			bigkey = new BigKey();
			bigkey.cmd = cmd;
			bigkey.key = keyStr;
			bigkey.size = requestSize > responseSize ? requestSize : responseSize;
			bigkey.lastUseTime = TimeUtil.currentTimeMillis();
			
			bigkeyMap.put(bigkey.key, bigkey);
			
		} else {
			if ( bigkey.count.get() >= Integer.MAX_VALUE ) {
				bigkey.count.set(1);
			}
			
			bigkey.cmd = cmd;
			bigkey.size = requestSize > responseSize ? requestSize : responseSize;
			bigkey.count.incrementAndGet();
			bigkey.lastUseTime = TimeUtil.currentTimeMillis();
			
			bigkeyMap.put(bigkey.key, bigkey);
		}
		
	}
	@Override
	public void onWatchType(String password, RedisRequestPolicy policy, String key, int requestSize) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onScheduleToZore() {
		bigkeyMap.clear();
	}


	@Override
	public void onSchedulePeroid(int peroid) {
	}
}
