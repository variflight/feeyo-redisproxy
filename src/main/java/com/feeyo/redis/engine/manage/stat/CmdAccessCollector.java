package com.feeyo.redis.engine.manage.stat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.util.FileUtils;

public class CmdAccessCollector extends AbstractStatCollector {
	
	private static final String COMMAND_COUNT_FILE_NAME = "command_count_v";
	private static final String COMMAND_PROC_TIME_FILE_NAME = "command_proc_time_v";
	
	// COMMAND、KEY
	private static ConcurrentHashMap<String, Command> commandCountMap = new ConcurrentHashMap<String, Command>();
	private static ConcurrentHashMap<String, AtomicLong> commandProcTimeMap = new ConcurrentHashMap<String, AtomicLong>();
	

	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills, boolean isCommandOnly ) {
	
		// 只有pipeline的子命令 是这种只收集指令数据的情况。
		if ( isCommandOnly ) {
			String pipelineCmd = RedisRequestType.PIPELINE.getCmd();
			Command parent = commandCountMap.get(pipelineCmd);
			if (parent == null) {
				parent = new Command();
				parent.cmd = pipelineCmd;
				commandCountMap.put(pipelineCmd, parent);
			}
			
			Command child = parent.getChild( cmd);
			if (child == null) {
				child = new Command();
				child.cmd = cmd;
				parent.addChild( child );
				
			} else {
				child.count.incrementAndGet();
			}
			return;
		}
		
		// Command 
		Command command = commandCountMap.get(cmd);
		if ( command != null ) {
			command.count.incrementAndGet();
		} else {
			command = new Command();
			command.cmd = cmd;
			commandCountMap.put(cmd, command);	
		}			
		
		
		// 计算指令消耗时间分布，5个档（小于5，小于10，小于20，小于50，大于50）
    	String timeKey = null;
    	if ( procTimeMills < 5 ) {
    		timeKey = "<5   ";
        } else if ( procTimeMills < 10 ) {
        	timeKey = "5-10 ";
        } else if ( procTimeMills < 20 ) {
        	timeKey = "10-20";
        } else if ( procTimeMills < 50 ) {
        	timeKey = "20-50";
        } else {
        	timeKey = ">50  ";
        }
    	
    	AtomicLong count = commandProcTimeMap.get(timeKey);
    	if (count == null) {
    		commandProcTimeMap.put(timeKey, new AtomicLong(1));
    	} else {
    		count.incrementAndGet();
    	}
		
	}

	@Override
	public void onScheduleToZore() {
		String date = getYesterdayDate();
		saveFile(date, false);
		commandCountMap.clear();
		commandProcTimeMap.clear();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	public ConcurrentHashMap<String, AtomicLong> getCommandProcTimeMap() {
    	return commandProcTimeMap;
    }

    public ConcurrentHashMap<String, Command> getCommandCountMap() {
    	return commandCountMap;
    }
    
    
    //
	public static class Command {
		
		public String cmd;
		public AtomicLong count = new AtomicLong(1);
		
		public Map<String, Command> childs = new ConcurrentHashMap<>();
		
		public Command getChild(String cmd) {
			return childs.get(cmd);
		}
		
		public void addChild(Command command) {
			childs.put(command.cmd, command);
		}
	}

	@Override
	protected void saveFile(String date, boolean isTemp) {
		storeCommandCountFile(date,isTemp);
		storeCommandProcTimeFile(date, isTemp);
	}

	private void storeCommandCountFile(String date, boolean isTemp) {
		if(commandCountMap.isEmpty())
			return;
		StringBuffer buffer = new StringBuffer();
		buffer.append("CMD").append(FIELD_SPARATOR).append("COUNT").append(LINE_SPARATOR);
		Set<Entry<String, Command>> entrys = commandCountMap.entrySet();
		for (Entry<String, Command> entry : entrys) {
			Command parent = entry.getValue();
			buffer.append(  parent.cmd ).append(FIELD_SPARATOR).append( parent.count.get()).append(LINE_SPARATOR);
			if ( parent.childs != null) {
				List<String> list = new ArrayList<String>();
				list.add( buffer.toString() );
				for (Entry<String, Command> childEntry : parent.childs.entrySet()) {
					Command child = childEntry.getValue();
					buffer.append(FIELD_SPARATOR).append( child.cmd ).append(FIELD_SPARATOR).append( child.count.get() ).append(LINE_SPARATOR);
				}
			}
		}
		String filename = basepath+COMMAND_COUNT_FILE_NAME+date+FILE_TYPE;
		FileUtils.ensureCreateFile(filename, buffer.toString(), isTemp);
	}
	
	private void storeCommandProcTimeFile(String date, boolean isTemp) {
		if(commandProcTimeMap.isEmpty())
			return;
		StringBuffer buffer = new StringBuffer();
		buffer.append("KEY").append(FIELD_SPARATOR).append("VALUE").append(LINE_SPARATOR);
		Collection<Entry<String, AtomicLong>> entrys = commandProcTimeMap.entrySet();
		for (Entry<String, AtomicLong> entry : entrys) {
			buffer.append(entry.getKey()).append(FIELD_SPARATOR).append(entry.getValue().get()).append(LINE_SPARATOR);
		}
		String filename = basepath+COMMAND_PROC_TIME_FILE_NAME+date+FILE_TYPE;
		FileUtils.ensureCreateFile(filename, buffer.toString(), isTemp);
	}

}
