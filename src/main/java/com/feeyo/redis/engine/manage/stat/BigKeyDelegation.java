package com.feeyo.redis.engine.manage.stat;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.manage.stat.BigKeyCollector.BigKey;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.FileUtils;

public class BigKeyDelegation implements Comparator<String> {

	private static Logger LOGGER = LoggerFactory.getLogger(BigKeyDelegation.class);

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
	private static final String LINE_SPARATOR = System.getProperty("line.separator");
	private static final String FILE_TYPE = ".txt";
	private static final String FIELD_SPARATOR = "  |  ";
	private static final String BIG_KEY_FILE_NAME = "big_key_v";
	private static final String basepath = System.getProperty("FEEYO_HOME") + "\\store\\discard\\";
	
	// bigkey 内存转磁盘清理限制
	private static int MEMORY_TO_FILE_LIMIT_LENGTH = 500;
	private static int MEMORY_TO_FILE_LIMIT_TIME = 5 * 60 * 1000;
	private static int MEMORY_TO_FILE_LIMIT_SIZE = 1 * 1024 * 1024;
	private static int MEMORY_TO_FILE_LIMIT_COUNT = 5;
	
	private static ConcurrentHashMap<String, BigKey> bigkeyMap = new ConcurrentHashMap<String, BigKey>(); 
	private static ConcurrentHashMap<String, BigKey> backupMap = new ConcurrentHashMap<String, BigKey>(); //size达到100个写入一次文件
	private static Map<String, BigKey> sortedMap;	//有序map
	
	public BigKeyDelegation() {
		sortedMap = Collections.synchronizedMap(new TreeMap<String, BigKey>(this));
	}

	/*
	 * 按照bigkey的size降序排列，在按照key的自然顺序升序排列
	 */
	@Override
	public int compare(String key1, String key2) {
		BigKey bigkey1 = bigkeyMap.get(key1);
		BigKey bigkey2 = bigkeyMap.get(key2);
		if (bigkey1 == bigkey2)
			return 1;
		else if (bigkey1.size > bigkey2.size)
			return -1;
		else if (bigkey1.size == bigkey2.size) {
			if (null == bigkey1.key)
				return -1;
			return bigkey1.key.compareTo(bigkey2.key);
		}
		return 1;
	}
	
	public ConcurrentHashMap<String, BigKey> getBigkeyMap() {
		return bigkeyMap;
	}
	
	public void doOnCollect(String cmd, String keyStr, int requestSize, int responseSize) {
		storeUnusedBigkey2File();
		storeBigkey2Mem(cmd, keyStr, requestSize, responseSize);
	}
	
	public void doAtZero() {
		String date = getYesterdayDate();
		saveFile(date, false);
		clear();
	}
	
	private BigKey put(ConcurrentHashMap<String, BigKey> map, BigKey bigkey) {
		if(null == bigkey || null == map)
			return null;
		BigKey bigkey2 = map.get(bigkey.key);
		if (bigkey2 == null)
			bigkey2 = bigkey;
		else {
			bigkey2.size = bigkey.size;
			bigkey2.count = new AtomicInteger(bigkey2.count.addAndGet(bigkey.count.get()));
			if (bigkey2.count.get() >= Integer.MAX_VALUE) {
				bigkey2.count.set(1);
			}
			bigkey2.lastUseTime = bigkey.lastUseTime;
		}
		return map.put(bigkey2.key, bigkey2);
	}
	
	/*
	 * src为最新数据，合并进dest
	 */
	private ConcurrentHashMap<String, BigKey> putAll(
			ConcurrentHashMap<String, BigKey> src,ConcurrentHashMap<String, BigKey> dest) {
		for (Entry<String, BigKey> entry : src.entrySet()) 
			put(dest, entry.getValue());
		return dest;
	}
	
	private synchronized void clear() {
		bigkeyMap.clear();
		backupMap.clear();
		sortedMap.clear();
	}

	private void storeUnusedBigkey2File() {
		if (bigkeyMap.size() > MEMORY_TO_FILE_LIMIT_LENGTH) {
			for (Entry<String, BigKey> entry : bigkeyMap.entrySet()) {
				BigKey bigKey = entry.getValue();
				// 清除： 最近5分钟没有使用过 && 使用总次数小于5 && 小于1M
				if (TimeUtil.currentTimeMillis() - bigKey.lastUseTime > MEMORY_TO_FILE_LIMIT_TIME && bigKey.count.get() < MEMORY_TO_FILE_LIMIT_COUNT
						&& bigKey.size < MEMORY_TO_FILE_LIMIT_SIZE) {
					backupMap.put(entry.getKey(), entry.getValue());
					if (backupMap.size() >= 100) {
						String date = sdf.format(new Date());
						saveFile(date, true);
						backupMap.clear();
					}
					bigkeyMap.remove(entry.getKey());
				}
			}
			LOGGER.info("bigkey clear. after clear bigkey length is :" + bigkeyMap.size());
		}
		
		// 如果清理内存转移到磁盘只清理掉50个key。说明转移的规则需要改变。
		if (bigkeyMap.size() > MEMORY_TO_FILE_LIMIT_LENGTH - 50) {
			MEMORY_TO_FILE_LIMIT_TIME = MEMORY_TO_FILE_LIMIT_TIME - (30 * 1000);
			MEMORY_TO_FILE_LIMIT_COUNT = MEMORY_TO_FILE_LIMIT_COUNT + 5;
			MEMORY_TO_FILE_LIMIT_SIZE = MEMORY_TO_FILE_LIMIT_SIZE + (1 * 1024 * 1024);
		}
	}
	
	private void storeBigkey2Mem(String cmd, String keyStr, int requestSize, int responseSize) {
		BigKey bigkey = new BigKey();
		bigkey.cmd = cmd;
		bigkey.key = keyStr;
		bigkey.size = requestSize > responseSize ? requestSize : responseSize;
		bigkey.lastUseTime = TimeUtil.currentTimeMillis();
		bigkey = put(bigkeyMap, bigkey);
	}
	
	private void saveFile(String date ,boolean isTemp) {
		if(bigkeyMap.isEmpty())
			return;
		
		StringBuffer buffer = new StringBuffer();
		Set<Entry<String, BigKey>> entrys;
		if(!isTemp) {
			//0点保存文件的时候进行排序
			sortAtZero(date);
			entrys = sortedMap.entrySet();
			buffer.append("CMD"+FIELD_SPARATOR+"KEY"+FIELD_SPARATOR+"SIZE"+FIELD_SPARATOR+"COUNT"+LINE_SPARATOR);
		}else
			entrys = backupMap.entrySet();
		
		for(Entry<String, BigKey> e : entrys) {
			BigKey bigkey = e.getValue();
			buffer.append( bigkey.cmd ).append(FIELD_SPARATOR).append( bigkey.key )
				.append( FIELD_SPARATOR ).append( bigkey.size )
				.append( FIELD_SPARATOR ).append( bigkey.count.get() ).append(LINE_SPARATOR);
		}
		String filename = basepath+BIG_KEY_FILE_NAME+date+FILE_TYPE;
		FileUtils.ensureCreateFile(filename, buffer.toString(), isTemp);
	}
	
	private ConcurrentHashMap<String, BigKey> getBigKeyMapFromFile(String date) {
		ConcurrentHashMap<String, BigKey> bigkeyStatTempBackup = new ConcurrentHashMap<String, BigKey>();
		List<String> lines = getLinesFromFile(date);
		for (String line : lines) {
			line = line.replace(FIELD_SPARATOR, FIELD_SPARATOR.trim());
			String[] items = line.split("\\|");
			if (items.length == 4) {
				BigKey bigkey = new BigKey();
				bigkey.cmd = items[0];
				bigkey.key = items[1];
				bigkey.size = Integer.parseInt(items[2]);
				bigkey.count = new AtomicInteger(Integer.parseInt(items[3]));
				BigKey bigkey2 = put(bigkeyStatTempBackup, bigkey);
				if(null != bigkey2)
					bigkey2.size = bigkey.size;
			}
		}
		return bigkeyStatTempBackup;
	}
	
	//排序 包括mem+file
	private void sortAtZero(String date) {
		bigkeyMap = putAll(bigkeyMap, backupMap);
		bigkeyMap = putAll(bigkeyMap, getBigKeyMapFromFile(date));
		sortedMap.clear();
		sortedMap.putAll(bigkeyMap);
	}

	private List<String> getLinesFromFile(String date) {
		String filename = basepath+BIG_KEY_FILE_NAME+date+FILE_TYPE;
		return FileUtils.readFile(filename);
	}

	private String getYesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		return sdf.format(cal.getTime());
	}

}
