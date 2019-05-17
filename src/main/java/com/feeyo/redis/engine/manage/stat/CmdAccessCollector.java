package com.feeyo.redis.engine.manage.stat;

import com.feeyo.net.codec.redis.RedisRequestPolicy;
import com.feeyo.net.codec.redis.RedisRequestType;
import com.feeyo.redis.net.front.handler.CommandParse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CmdAccessCollector implements StatCollector {
	
	// COMMAND、KEY
	private static ConcurrentHashMap<String, Command> commandCountMap = new ConcurrentHashMap<String, Command>();
	private static ConcurrentHashMap<String, UserCommand> userCommandCountMap = new ConcurrentHashMap<String, UserCommand>();
	private static ConcurrentHashMap<String, AtomicLong> commandProcTimeMap = new ConcurrentHashMap<String, AtomicLong>();
	private static ConcurrentHashMap<String, AtomicLong> commandWaitTimeMap = new ConcurrentHashMap<String, AtomicLong>();
	

	@Override
	public void onCollect(String password, String cmd, String key, int requestSize, int responseSize, int procTimeMills, int waitTimeMills, boolean isCommandOnly, boolean isBypass ) {

		UserCommand userCommand = userCommandCountMap.get(password);
		if ( userCommand == null ) {
			userCommand = new UserCommand( password );
			userCommandCountMap.put(password, userCommand);
		}
		
		// 只有pipeline的子命令 是这种只收集指令数据的情况。
		if ( isCommandOnly ) {
			String pipelineCmd = RedisRequestType.PIPELINE.getCmd();
			Command parent = commandCountMap.get(pipelineCmd);
			if (parent == null) {
				parent = new Command();
				parent.cmd = pipelineCmd;
				commandCountMap.put(pipelineCmd, parent);
			}
			
			Command child = parent.getChild( cmd );
			if (child == null) {
				child = new Command();
				child.cmd = cmd;
				parent.addChild( child );
				
			} else {
				child.count.incrementAndGet();
			}
			
			userCommand.incrementCommandCount( cmd );
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
		
		userCommand.incrementCommandCount( cmd );
		
		// 计算指令消耗
		timeMillsCollect(procTimeMills, commandProcTimeMap);
		// 计算等待消耗
		timeMillsCollect(waitTimeMills, commandWaitTimeMap);
		
	}

	private void timeMillsCollect(int timeMills, ConcurrentHashMap<String, AtomicLong> timeMap) {
		// 计算指令消耗时间分布，5个档（小于5，小于10，小于20，小于50，大于50）
		String timeKey = null;
		if (timeMills < 5) {
			timeKey = "<5   ";
		} else if (timeMills < 10) {
			timeKey = "5-10 ";
		} else if (timeMills < 20) {
			timeKey = "10-20";
		} else if (timeMills < 50) {
			timeKey = "20-50";
		} else {
			timeKey = ">50  ";
		}

		AtomicLong count = timeMap.get(timeKey);
		if (count == null) {
			timeMap.put(timeKey, new AtomicLong(1));
		} else {
			count.incrementAndGet();
		}
	}

	@Override
	public void onScheduleToZore() {
		commandCountMap.clear();
		userCommandCountMap.clear();
		commandProcTimeMap.clear();
		commandWaitTimeMap.clear();
	}

	@Override
	public void onSchedulePeroid(int peroid) {
	}
	
	public ConcurrentHashMap<String, AtomicLong> getCommandProcTimeMap() {
    		return commandProcTimeMap;
    }
	
	public ConcurrentHashMap<String, AtomicLong> getCommandWaitTimeMap() {
		return commandWaitTimeMap;
	}

    public ConcurrentHashMap<String, Command> getCommandCountMap() {
    		return commandCountMap;
    }
    
    public ConcurrentHashMap<String, UserCommand> getUserCommandCountMap() {
    		return userCommandCountMap;
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
	
	public static class UserCommand {
		public String user;
		public AtomicLong writeCommandCount;
		public AtomicLong readComandCount;
		public ConcurrentHashMap<String, AtomicLong> commandCount = new ConcurrentHashMap<String, AtomicLong>();
		
		public UserCommand(String user) {
			this.user = user;
			writeCommandCount = new AtomicLong(0);
			readComandCount = new AtomicLong(0);
		}
		
		public void incrementCommandCount(String cmd) {
			RedisRequestPolicy policy = CommandParse.getPolicy(cmd);
			if ( policy.isRead() ) {
				readComandCount.incrementAndGet();
			} else {
				writeCommandCount.incrementAndGet();
			}
			
			AtomicLong count = commandCount.get(cmd);
			if (count == null) {
				count = new AtomicLong(1);
				commandCount.put(cmd, count);
			} else {
				count.incrementAndGet();
			}
		}
	}

}
