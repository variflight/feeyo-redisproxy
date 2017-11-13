package com.feeyo.redis.engine.manage.stat;

import java.util.Calendar;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.NetSystem;
import com.feeyo.redis.nio.util.TimeUtil;

/**
 * 数据埋点收集器
 * 
 * @author zhuam
 *
 */
public class StatUtil {
	
	private static Logger LOGGER = LoggerFactory.getLogger( StatUtil.class );
	
	public static final String STAT_KEY = "RedisProxyStat";
	private final static String PIPELINE_CMD = "pipeline";
	private final static long DAY_MILLIS = 24*60*60*1000;
	
	public static final int BIGKEY_SIZE = 1024 * 256;  				// 大于 256K
	
	public static final int SLOW_COST = 50; 			  			// 50秒	
	public static final int STATISTIC_PEROID = 30; 		  			// 30秒

	// COMMAND、KEY
	private static ConcurrentHashMap<String, Command> commandStats = new ConcurrentHashMap<String, Command>();
	private static ConcurrentHashMap<String, BigKey> bigkeyStats = new ConcurrentHashMap<String, BigKey>();
	private static ConcurrentHashMap<String, AtomicLong> procTimeMillsDistribution = new ConcurrentHashMap<String, AtomicLong>();
	
	// ACCESS
	private static ConcurrentHashMap<String, AccessStatInfo> accessStats = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, AccessStatInfoResult> totalResults = new ConcurrentHashMap<>();
	
	private static ConcurrentHashMap<String, AccessDayStatInfo> accessDayStats = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, AccessStatInfoResult> totalDayResults = new ConcurrentHashMap<>();
	
	
	public static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	public static ScheduledFuture<?> scheduledFuture;
	
	public static long zeroTimeMillis = 0;
	public static long preZoreTimeMillis = 0; //当天零点零分
	
	static {
		scheduledFuture = executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {		
				//记录用户每天的流量
		        long currentTimeMillis = System.currentTimeMillis();
		        if( 0 == preZoreTimeMillis || currentTimeMillis - preZoreTimeMillis > DAY_MILLIS) {
		        	Calendar cal = Calendar.getInstance();
			        cal.set(Calendar.HOUR_OF_DAY, 0);
					cal.set(Calendar.MINUTE, 0);
					cal.set(Calendar.SECOND, 0); 
					cal.set(Calendar.MILLISECOND, 0); 	
					preZoreTimeMillis = cal.getTimeInMillis();
		        }
				long timeMillis = (currentTimeMillis -preZoreTimeMillis)/1000;
				int hour = (int)(timeMillis/3600%24);
				int minute = (int)(timeMillis%3600/60);
				
				for(Map.Entry<String, AccessDayStatInfo> entry : accessDayStats.entrySet()) {
		        	AccessDayStatInfo info = entry.getValue();
		        	if(minute != info.getCurrentHour().getMinute().get()) {
		        		info.getCurrentHour().addMinuteStatInfo(entry.getKey(),currentTimeMillis,minute);
		        		info.addHoursStatInfo(entry.getKey(),currentTimeMillis,hour);
			        	AccessStatInfoResult result = info.calculate(currentTimeMillis);
						totalDayResults.put(result.key, result);
		        	}
		        }
				
				// 定时计算
				currentTimeMillis = System.currentTimeMillis();
		        for (Map.Entry<String, AccessStatInfo> entry : accessStats.entrySet()) {     
					AccessStatInfo info = entry.getValue();
					AccessStatInfoResult result = info.calculate(currentTimeMillis, STATISTIC_PEROID);
					info.clear(currentTimeMillis, STATISTIC_PEROID);
					
					totalResults.put(result.key, result);
		        }  
		        
		        // 凌晨清理
		        if ( TimeUtil.currentTimeMillis() > zeroTimeMillis ) {
			       
		        	Calendar cal = Calendar.getInstance();
			        cal.set(Calendar.HOUR_OF_DAY, 23);
					cal.set(Calendar.MINUTE, 59);
					cal.set(Calendar.SECOND, 59); 
					cal.set(Calendar.MILLISECOND, 59); 	
					
					//
					if ( zeroTimeMillis > 0 ) {
						
						long sum = 0;
						Set<Entry<String, Command>> entrys = StatUtil.getCommandStats();
						for (Entry<String, Command> entry : entrys) {	
							Long count = entry.getValue().count.get();			
							if ( count != null )
								sum += count;
						}
						
						LOGGER.info("Through cmd count:" + sum);
						
						totalDayResults.clear();
						commandStats.clear();
						bigkeyStats.clear();
						procTimeMillsDistribution.clear();
						//setkeyStats.clear();
					}
					
					zeroTimeMillis = cal.getTimeInMillis();
		        }				
			}
		}, STATISTIC_PEROID, STATISTIC_PEROID, TimeUnit.SECONDS);
	}
	
	
	/**
	 * 收集
	 * 
	 * @param spot
	 * @param isCommandOnly 用于判断此次收集是否只用于command（pipeline指令的子指令）收集。
	 */
	public static void collect(final String password, final String cmd, final byte[] key, 
			final int requestSize, final int responseSize, final int procTimeMills, final boolean isCommandOnly) {
		
		if ( cmd == null ) {
			return;
		}
		
		// 线程池
		NetSystem.getInstance().getBusinessExecutor().execute( new Runnable() {
			
			@Override
			public void run() {
				
				// 只有pipeline的子命令 是这种只收集指令数据的情况。
				if ( isCommandOnly ) {
					
					Command parent = commandStats.get(PIPELINE_CMD);
					if (parent == null) {
						parent = new Command();
						parent.cmd = PIPELINE_CMD;
						commandStats.put(PIPELINE_CMD, parent);
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
				Command command = commandStats.get(cmd);
				if ( command != null ) {
					command.count.incrementAndGet();
				} else {
					command = new Command();
					command.cmd = cmd;
					commandStats.put(cmd, command);	
				}			
		        
				
				long currentTimeMillis = TimeUtil.currentTimeMillis();
				
		        // QPS、SLOW、BYTES
		        try {
		        	// password 
		            AccessStatInfo stat1 = getAccessStatInfo(password, currentTimeMillis);
		            stat1.collect(currentTimeMillis, procTimeMills, requestSize, responseSize);
		        	
		            // all
		            AccessStatInfo stat2 = getAccessStatInfo(STAT_KEY, currentTimeMillis);
		            stat2.collect(currentTimeMillis, procTimeMills, requestSize, responseSize);
		            
		            //user day net_io
		            AccessDayStatInfo stat3 = getAccessDayStatInfo(password, currentTimeMillis);
		            stat3.collect(currentTimeMillis, procTimeMills, requestSize, responseSize);
		            
		        } catch (Exception e) {
		        }
		        
		        // 计算指令消耗时间分布，5个档（小于5，小于10，小于20，小于50，大于50）
		        collectProcTimeMillsDistribution( procTimeMills );
		        
				// 大key
				if ( key != null && ( requestSize > BIGKEY_SIZE || responseSize > BIGKEY_SIZE ) ) {	
					
					if ( bigkeyStats.size() > 300 ) {
						bigkeyStats.clear();
					}
						
					
					String keyStr = new String( key );
					BigKey bigkey = bigkeyStats.get( keyStr );
					if ( bigkey == null ) {
						bigkey = new BigKey();
						bigkey.cmd = cmd;
						bigkey.key = keyStr;
						bigkey.size = requestSize > responseSize ? requestSize : responseSize;
						
						bigkeyStats.put(bigkey.key, bigkey);
						
					} else {
						if ( bigkey.count.get() >= Integer.MAX_VALUE ) {
							bigkey.count.set(1);
						}
						
						bigkey.cmd = cmd;
						bigkey.size = requestSize > responseSize ? requestSize : responseSize;
						bigkey.count.incrementAndGet();
						
						bigkeyStats.put(bigkey.key, bigkey);
					}
				}
			}
		});
	}
    
    private static AccessStatInfo getAccessStatInfo(String key, long currentTime) {
        AccessStatInfo item = accessStats.get(key);
        if (item == null) {
            accessStats.putIfAbsent(key, new AccessStatInfo(key, currentTime));
            item = accessStats.get(key);
        }
        return item;
    }
    
    private static AccessDayStatInfo getAccessDayStatInfo(String key, long currentTime) {
        AccessDayStatInfo item = accessDayStats.get(key);
        if (item == null) {
        	accessDayStats.putIfAbsent(key, new AccessDayStatInfo(key, currentTime));
            item = accessDayStats.get(key);
        }
        return item;
    }
    
    private static void collectProcTimeMillsDistribution(int procTimeMills) {
    	String key = null;
    	if ( procTimeMills < 5 ) {
    		key = "<5   ";
        } else if ( procTimeMills < 10 ) {
        	key = "5-10 ";
        } else if ( procTimeMills < 20 ) {
        	key = "10-20";
        } else if ( procTimeMills < 50 ) {
        	key = "20-50";
        } else {
        	key = ">50  ";
        }
    	
    	AtomicLong count = procTimeMillsDistribution.get(key);
    	if (count == null) {
    		procTimeMillsDistribution.put(key, new AtomicLong(1));
    	} else {
    		count.incrementAndGet();
    	}
    	
    }
    
    public static ConcurrentHashMap<String, AccessStatInfoResult> getTotalDayAccessStatInfo() {
		return totalDayResults;
	}
    
    public static ConcurrentHashMap<String, AccessStatInfoResult> getTotalAccessStatInfo() {
        return totalResults;
    }
    
    public static ConcurrentHashMap<String, BigKey> getBigKeyStats() {
    	return bigkeyStats;
    }
    
    public static ConcurrentHashMap<String, AtomicLong> getProcTimeMillsDistribution() {
    	return procTimeMillsDistribution;
    }

    public static Set<Entry<String, Command>> getCommandStats() {
    	return commandStats.entrySet();
    }
    
	public static class AccessStatInfoResult {
		public String key;
		public int  totalCount  = 0;
		public int  slowCount  = 0;
		public int  procTime = 0;
		public int  maxCount = -1;
		public int  minCount = -1;	
		public int avgCount = -1;
		
		public long[] netInBytes = new long[]{0,0,-1,0};  // 0 total 1 max 2 min 3 avg
		public long[] netOutBytes = new long[]{0,0,-1,0};
		
		public long created;
	}
	
	public static class BigKey {
		public String cmd;
		public String key;
		public int size;
		public AtomicInteger count = new AtomicInteger(1);
	}
	
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
