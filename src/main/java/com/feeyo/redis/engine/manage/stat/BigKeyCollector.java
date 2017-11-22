package com.feeyo.redis.engine.manage.stat;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.FileUtils;

public class BigKeyCollector extends AbstractStatCollector {
	
	private static Logger LOGGER = LoggerFactory.getLogger( BigKeyCollector.class );
	
	public static final int BIGKEY_SIZE = 1024 * 256;  				// 大于 256K
	
	private static ConcurrentHashMap<String, BigKey> bigkeyMap = new ConcurrentHashMap<String, BigKey>();
	private static ConcurrentHashMap<String, BigKey> bigkeyMapBackup = new ConcurrentHashMap<String, BigKey>();
	
	private static final String BIG_KEY_FILE_NAME = "big_key_v";
	
	public ConcurrentHashMap<String, BigKey> getBigkeyMap() {
		return bigkeyMap;
	}

	
	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, 
			int procTimeMills, boolean isCommandOnly ) {
		
		// 大key 校验
		if (  requestSize < BIGKEY_SIZE && responseSize < BIGKEY_SIZE  ) {	
			return;
		}
		
		
		if ( bigkeyMap.size() > 500 ) {
			for (Entry<String, BigKey> entry : bigkeyMap.entrySet()) {
				BigKey bigKey = entry.getValue();
				// TODO 后序优化
				// 清除： 最近5分钟没有使用过 && 使用总次数小于5 && 小于1M
				if (TimeUtil.currentTimeMillis() - bigKey.lastUseTime > 5 * 60 * 1000
						&& bigKey.count.get() < 5 && bigKey.size < 1 * 1024 * 1024) {
					bigkeyMapBackup.put(entry.getKey(), entry.getValue());
					if(bigkeyMapBackup.size() >= 100) {
						String date = sdf.format(new Date());
						saveFile(date, true);
						bigkeyMapBackup.clear();
					}
					
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
	public void onScheduleToZore() {
		String date = getYesterdayDate();
		mergeBigkeyMaps(date);
		saveFile(date, false);
		bigkeyMapBackup.clear();
		bigkeyMap.clear();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	
	public static class BigKey {
		public String cmd;
		public String key;
		public int size;
		public AtomicInteger count = new AtomicInteger(1);
		public long lastUseTime;
	}


	@Override
	protected void saveFile(String date ,boolean isTemp) {
		if(bigkeyMap.isEmpty())
			return;
		StringBuffer buffer = new StringBuffer();
		Collection<Entry<String, BigKey>> entrys = bigkeyMap.entrySet();
		for(Entry<String, BigKey> e : entrys) {
			BigKey bigkey = e.getValue();
			buffer.append( bigkey.cmd ).append(FIELD_SPARATOR).append( bigkey.key )
				.append( FIELD_SPARATOR ).append( bigkey.size )
				.append( FIELD_SPARATOR ).append( bigkey.count.get() ).append(LINE_SPARATOR);
		}
		String filename = basepath+BIG_KEY_FILE_NAME+date;
		FileUtils.ensureCreateFile(filename, buffer.toString(), isTemp);
	}

	private void mergeBigkeyMaps(String date) {
		ConcurrentHashMap<String, BigKey> tempBigkeystats = transformFile2BigkeyMap(date);
		bigkeyMap = mergeBigkeyMap(bigkeyMap, bigkeyMapBackup);
		bigkeyMap = mergeBigkeyMap(bigkeyMap, tempBigkeystats);
	}

	private ConcurrentHashMap<String, BigKey> mergeBigkeyMap(ConcurrentHashMap<String, BigKey> dest,ConcurrentHashMap<String, BigKey> src){
		Set<Entry<String, BigKey>> entrys = src.entrySet();
		for(Entry<String,BigKey>entry : entrys) {
			BigKey bigkey = dest.get(entry.getKey());
			if(entry.getValue() == null)
				continue;
			if(bigkey == null)
				dest.put(entry.getKey(), entry.getValue());
			else {
				//dest为最新的，故不需要更新size
				bigkey.count = new AtomicInteger(bigkey.count.addAndGet(entry.getValue().count.get()));
			}
		}
		return dest;
	}
	
	private ConcurrentHashMap<String, BigKey> transformFile2BigkeyMap(String date){
		ConcurrentHashMap<String, BigKey> bigkeyStatTempBackup = new ConcurrentHashMap<String, BigKey>();
		List<String> lines = getBigkeyBackUp(date);
		for(String line : lines) {
			String[] items = line.split(FIELD_SPARATOR);
			if(items.length == 4) {
				BigKey bigkey = new BigKey();
				bigkey.cmd = items[0];
				bigkey.key = items[1];
				bigkey.size = Integer.parseInt(items[2]);
				bigkey.count = new AtomicInteger(Integer.parseInt(items[3]));
				BigKey bigkey2 = bigkeyStatTempBackup.get(bigkey.key);
				if(null == bigkey2) {
					bigkeyStatTempBackup.put(bigkey.key, bigkey);
				}
				else {
					bigkey2.count = new AtomicInteger(bigkey2.count.addAndGet(bigkey.count.get()));
					bigkey2.size = bigkey.size;
				}
			}
		}
		return bigkeyStatTempBackup;
	}
	
	private List<String> getBigkeyBackUp(String date) {
		String filename = basepath+BIG_KEY_FILE_NAME+date;
		return FileUtils.readFile(filename);
	}
	
}
