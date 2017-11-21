package com.feeyo.redis.engine.manage.stat;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.manage.stat.BigKeyCollector.BigKey;
import com.feeyo.redis.engine.manage.stat.CmdAccessCollector.Command;
import com.feeyo.redis.engine.manage.stat.NetFlowCollector.UserNetFlow;
import com.feeyo.util.FileUtils;

public class DataBackupHandler {
	
	private static Logger LOGGER = LoggerFactory.getLogger(DataBackupHandler.class);
	
	private static String basepath ;
	private static final String LINE_SPARATOR = System.getProperty("line.separator");
	private static final String FIELD_SPARATOR = "  |  ";
	private static final String VERSION_FLAG = "_v";
	
	private static final String COMMAND_COUNT_FILE_NAME = "command_count"+VERSION_FLAG;
	private static final String BIG_KEY_FILE_NAME = "big_key"+VERSION_FLAG;
	private static final String USER_NET_FLOW_FILE_NAME = "user_net_flow"+VERSION_FLAG;
	private static final String COMMAND_PROC_TIME_FILE_NAME = "command_proc_time"+VERSION_FLAG;
	
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
	
	static {
		if( null == System.getProperty("FEEYO_HOME"))
			System.setProperty("FEEYO_HOME",System.getProperty("user.dir"));
		basepath = System.getProperty("FEEYO_HOME")+File.separator+"store"+File.separator+"databackup"+File.separator;
	}
	
	//以日期为版本号 eg：2017_11_17
	public static boolean storeCommandCountFile(ConcurrentHashMap<String, Command> commandStats,String  version) {
		if(commandStats.isEmpty())
			return false;
		StringBuffer buffer = new StringBuffer();
		Set<Entry<String, Command>> entrys = commandStats.entrySet();
		for (Entry<String, Command> entry : entrys) {
			Command parent = entry.getValue();
			buffer.append(  parent.cmd ).append(FIELD_SPARATOR).append( parent.count.get()).append(LINE_SPARATOR);
			if ( parent.childs != null) {
				List<String> list = new ArrayList<String>();
				list.add( buffer.toString() );
				for (Entry<String, Command> childEntry : parent.childs.entrySet()) {
					Command child = childEntry.getValue();
					StringBuffer sb = new StringBuffer();
					sb.append(FIELD_SPARATOR).append( child.cmd ).append(FIELD_SPARATOR).append( child.count.get() ).append(LINE_SPARATOR);
				}
			}
		}
		String filename = basepath+COMMAND_COUNT_FILE_NAME+version;
		return FileUtils.ensureCreateFile(filename, buffer.toString(), false);
	}
	
	//临时备份以追加的方式写入文本
	public static boolean storeBigkeyFile(ConcurrentHashMap<String, BigKey> bigkeyStats, String version,boolean isTemp) {
		if(bigkeyStats.isEmpty())
			return false;
		StringBuffer buffer = new StringBuffer();
		Collection<Entry<String, BigKey>> entrys = bigkeyStats.entrySet();
		for(Entry<String, BigKey> e : entrys) {
			BigKey bigkey = e.getValue();
			buffer.append( bigkey.cmd ).append(FIELD_SPARATOR).append( bigkey.key ).append( FIELD_SPARATOR ).append( bigkey.size ).append( FIELD_SPARATOR ).append( bigkey.count.get() ).append(LINE_SPARATOR);
		}
		String filename = basepath+BIG_KEY_FILE_NAME+version;
		return FileUtils.ensureCreateFile(filename, buffer.toString(), isTemp);
	}
	
	public static boolean storeUserNetFlowFile(ConcurrentHashMap<String, UserNetFlow> userNetIoStats, String version) {
		if(userNetIoStats.isEmpty())
			return false;
		StringBuffer buffer = new StringBuffer();
		long totalNetIn = 0;
		long totalNetOut = 0;
		Set<Entry<String, UserNetFlow>> entrys = userNetIoStats.entrySet();
		buffer.append("User").append(FIELD_SPARATOR).append("NetIn").append(FIELD_SPARATOR).append("NetOut").append(LINE_SPARATOR);
		for (Map.Entry<String, UserNetFlow> entry : entrys) { 
			if (!StatUtil.STAT_KEY.equals(entry.getKey())) {
				UserNetFlow userNetIo = entry.getValue();
				buffer.append(userNetIo.password).append(FIELD_SPARATOR).append(userNetIo.netIn.get()).append(FIELD_SPARATOR).append(userNetIo.netOut.get()).append(LINE_SPARATOR);
				totalNetIn = totalNetIn + userNetIo.netIn.get();
				totalNetOut = totalNetOut + userNetIo.netOut.get();
			}
		}
		buffer.append("total").append(FIELD_SPARATOR).append(totalNetIn).append(FIELD_SPARATOR).append(totalNetOut).append(LINE_SPARATOR);
		
		String filename = basepath+USER_NET_FLOW_FILE_NAME+version;
		return FileUtils.ensureCreateFile(filename, buffer.toString(), false);
	}
	
	public static boolean storeCommandProcTimeFile(ConcurrentHashMap<String, AtomicLong> commandProcTime, String version) {
		if(commandProcTime.isEmpty())
			return false;
		StringBuffer buffer = new StringBuffer();
		Collection<Entry<String, AtomicLong>> entrys = commandProcTime.entrySet();
		for (Entry<String, AtomicLong> entry : entrys) {
			buffer.append(entry.getKey()).append(FIELD_SPARATOR).append(entry.getValue().get()).append(LINE_SPARATOR);
		}
		String filename = basepath+COMMAND_PROC_TIME_FILE_NAME+version;
		return FileUtils.ensureCreateFile(filename, buffer.toString(), false);
	}
	
	public static List<String> getCommandCountBackUp(String version) {
		String filename = basepath+COMMAND_COUNT_FILE_NAME+version;
		return FileUtils.readFile(filename);
	}
	
	public static List<String> getBigkeyBackUp(String version) {
		String filename = basepath+BIG_KEY_FILE_NAME+version;
		return FileUtils.readFile(filename);
	}
	
	public static List<String> getUserNetFlowBackup(String version) {
		String filename = basepath+USER_NET_FLOW_FILE_NAME+version;
		return FileUtils.readFile(filename);
	}
	
	public static List<String> getCommandProcTimeBackup(String version) {
		String filename = basepath+COMMAND_PROC_TIME_FILE_NAME+version;
		return FileUtils.readFile(filename);
	}
	
	//对bigkey的temp文件处理
	public static ConcurrentHashMap<String, BigKey> transformFile2BigkeyMap(String version){
		ConcurrentHashMap<String, BigKey> bigkeyStatTempBackup = new ConcurrentHashMap<String, BigKey>();
		List<String> lines = getBigkeyBackUp(version);
		long totalNum = 0;
		long mergeNum = 0;
		long restNum = 0;
		for(String line : lines) {
			totalNum ++;
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
					restNum ++;
				}
				else {
					bigkey2.count = new AtomicInteger(bigkey2.count.addAndGet(bigkey.count.get()));
					bigkey2.size = bigkey.size;
					mergeNum ++;
				}
			}
		}
		LOGGER.info("Before Zero: TempFileVersion:{}; Total BigKey Num : {}; Merge BigKey Num : {}; Rest BigKey Num : {};", version, totalNum, mergeNum,restNum);
		return bigkeyStatTempBackup;
	}
	
	
	public static ConcurrentHashMap<String, BigKey> mergeBigkeyMap(ConcurrentHashMap<String, BigKey> dest,ConcurrentHashMap<String, BigKey> src){
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

	public static void cleanDataBackupBeforeOneMonth() {
		String[] filenames = FileUtils.getFilenamesUnderDir(basepath);
		String[] cleanVersions = filterFilenames(filenames);
		for(int i = 0; i < cleanVersions.length; i++)
			cleanVersions[i] = basepath + cleanVersions[i];
		FileUtils.cleanFiles(cleanVersions);
	}

	//过滤出一个月前的文件
	private static String[] filterFilenames(String[] filenames) {
		List<String> list = new ArrayList<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.MONTH, -1);
		String beforeOneMonthVersion = sdf.format(cal.getTime());
		for(String filename : filenames) {
			String fileversion = filename.substring(filename.lastIndexOf(VERSION_FLAG)+VERSION_FLAG.length());
			if(beforeOneMonthVersion.compareTo(fileversion) >= 0)
				list.add(filename);
		}
		return list.toArray(new String[0]);
	}
}
