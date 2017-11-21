package com.feeyo.redis.engine.manage.stat;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CmdAccessCollector implements StatCollector {
	
	private final static String PIPELINE_CMD = "pipeline";
	
	// COMMAND、KEY
	private static ConcurrentHashMap<String, Command> commandCountMap = new ConcurrentHashMap<String, Command>();
	private static ConcurrentHashMap<String, AtomicLong> commandProcTimeMap = new ConcurrentHashMap<String, AtomicLong>();
	

	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills, boolean isCommandOnly ) {
	
		// 只有pipeline的子命令 是这种只收集指令数据的情况。
		if ( isCommandOnly ) {
			
			Command parent = commandCountMap.get(PIPELINE_CMD);
			if (parent == null) {
				parent = new Command();
				parent.cmd = PIPELINE_CMD;
				commandCountMap.put(PIPELINE_CMD, parent);
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

}
