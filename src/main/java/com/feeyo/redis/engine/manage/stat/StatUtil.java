package com.feeyo.redis.engine.manage.stat;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.manage.stat.BigKeyCollector.BigKey;
import com.feeyo.redis.engine.manage.stat.BigLengthCollector.BigLength;
import com.feeyo.redis.engine.manage.stat.CmdAccessCollector.Command;
import com.feeyo.redis.engine.manage.stat.CmdAccessCollector.UserCommand;
import com.feeyo.redis.engine.manage.stat.NetFlowCollector.UserNetFlow;
import com.feeyo.redis.engine.manage.stat.SlowKeyColletor.SlowKey;
import com.feeyo.redis.nio.NetSystem;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.MailUtil;
import com.feeyo.util.NetworkUtil;

/**
 * 数据埋点收集器
 * 
 * @author zhuam
 *
 */
public class StatUtil {
	
	private static Logger LOGGER = LoggerFactory.getLogger( StatUtil.class );
	
	public static final String STAT_KEY = "RedisProxyStat";


	
	public static final int SLOW_COST = 50; 			  			// 超过50毫秒	
	public static final int STATISTIC_PEROID = 30; 		  			// 30秒
	
	// ACCESS
	private static ConcurrentHashMap<String, AccessStatInfo> accessStats = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, AccessStatInfoResult> totalResults = new ConcurrentHashMap<>();
	
	public static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	public static ScheduledFuture<?> scheduledFuture;
	
	public static long zeroTimeMillis = 0;
	
	// 收集器
	private static List<StatCollector> collectors = new CopyOnWriteArrayList<>();
	
	private static NetFlowCollector netflowCollector = new NetFlowCollector();
	private static CmdAccessCollector cmdAccessCollector = new CmdAccessCollector();
	private static BigKeyCollector bigKeyCollector = new BigKeyCollector();
	private static BigLengthCollector bigLengthCollector = new BigLengthCollector();
	private static SlowKeyColletor slowKeyCollector = new SlowKeyColletor();
	
	static {
		
		addCollector( netflowCollector );
		addCollector( cmdAccessCollector );
		addCollector( bigKeyCollector );
		addCollector( bigLengthCollector );
		addCollector( slowKeyCollector );
		
		scheduledFuture = executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {		
				
				// 定时计算
				long currentTimeMillis = System.currentTimeMillis();
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
						
						// send mail
						// ##################################################################################
						try {
							
							
							StringBuffer subject = new StringBuffer( 50 );
							subject.append(" ###RedisProxy report, host:" ).append( NetworkUtil.getLocalIp() );
							
							StringBuffer body = new StringBuffer( 500 );
							
							// COMMAND 
							long sum = 0;
							body.append("#############   command asscess  #################\n");
							body.append("|    cmd    |     count     |");
							for (Command command : cmdAccessCollector.getCommandCountMap().values() ) {
								body.append("\n");
								body.append("|    ");
								body.append(command.cmd).append("    |    ");
								body.append(command.count.get() ).append("    |    ");
	
								sum += command.count.get();
							}
							body.append("\r\n");
							body.append("\r\n");
							
							LOGGER.info("Through cmd count:" + sum);
							
							
							// SLOW KEY
							body.append("#############   slowkey status ( >50ms )   #################\n");
							body.append("|    cmd    |     key     |    count    |");
							for (SlowKey slowKey : slowKeyCollector.getSlowKeys() ) {
								body.append("\n");
								body.append("|    ");
								body.append(slowKey.cmd).append("    |    ");
								body.append(slowKey.key).append("    |    ");
								body.append(slowKey.count).append("    |");
							}
							body.append("\r\n");
							body.append("\r\n");
							
							// BIG KEY
							body.append("#############   bigkey status   #################\n");
							body.append("|    cmd    |     key     |    size    |    count    |");
							for (BigKey bigkey : bigKeyCollector.getBigkeys() ) {
								body.append("\n");
								body.append("|    ");
								body.append(bigkey.cmd).append("    |    ");
								body.append(bigkey.key).append("    |    ");
								body.append(bigkey.size).append("    |    ");
								body.append(bigkey.count).append("    |");
							}
							body.append("\r\n");
							body.append("\r\n");
	
							// BIG LENGTH
							body.append("#############   biglenght status   #################\n");
							body.append("|    key    |     type     |    length    |    count_1k    |    count_10k    |");
							for (BigLength bigLength : bigLengthCollector.getBigLengthMap().values() ) {
								body.append("\n");
								body.append("|    ");
								body.append(bigLength.key).append("    |    ");
								body.append(bigLength.cmd).append("    |    ");
								body.append(bigLength.length).append("    |    ");
								body.append(bigLength.count_1k).append("    |    ");
								body.append(bigLength.count_10k).append("    |");
							}
							body.append("\r\n");
							body.append("\r\n");
							
							String[] attachments = null;
							
							Properties prop = RedisEngineCtx.INSTANCE().getMailProperties();
							MailUtil.send(prop, subject.toString(), body.toString(), attachments);
							
							
						} catch(Throwable t) {
							//ignore
						}
						// ##################################################################################
						
						
						// 触发0 点事件
						for(StatCollector listener: collectors) {
							try {
								listener.onScheduleToZore();
							} catch(Exception e) {
								LOGGER.error("error:",e);
							}
						}
					}
					
					zeroTimeMillis = cal.getTimeInMillis();
		        }
		        
		        
		        for(StatCollector listener: collectors) {
					try {
						listener.onSchedulePeroid( STATISTIC_PEROID );
					} catch(Exception e) {
						LOGGER.error("error:",e);
					}
				}
		        
				
			}
		}, STATISTIC_PEROID, STATISTIC_PEROID, TimeUnit.SECONDS);
	}
	
	
	public static void addCollector(StatCollector collector) {
		if (collector == null) {
			throw new NullPointerException();
		}
		collectors.add(collector);
	}
	
	public static void removeCollector(StatCollector collector) {
		collectors.remove(collector);
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
				
				String keyStr = new String(key);
				for(StatCollector listener: collectors) {
					try {
						listener.onCollect(password, cmd, keyStr, requestSize, responseSize, procTimeMills, isCommandOnly);
					} catch(Exception e) {
						LOGGER.error("error:",e);
					}
				}	
				
				if (isCommandOnly) {
					return;
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
		            
		        } catch (Exception e) {
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
    
    public static ConcurrentHashMap<String, AccessStatInfoResult> getTotalAccessStatInfo() {
        return totalResults;
    }
    
    public static List<BigKey> getBigKeys() {
    	return bigKeyCollector.getBigkeys();
    }
    
    public static ConcurrentHashMap<String, BigLength> getBigLengthMap() {
    	return bigLengthCollector.getBigLengthMap();
    }
    
    public static ConcurrentHashMap<String, AtomicLong> getCommandProcTimeMap() {
    	return cmdAccessCollector.getCommandProcTimeMap();
    }

    public static ConcurrentHashMap<String, Command> getCommandCountMap() {
    	return cmdAccessCollector.getCommandCountMap();
    }
    
    public static ConcurrentHashMap<String, UserCommand> getUserCommandCountMap() {
    	return cmdAccessCollector.getUserCommandCountMap();
    }
    
    public static ConcurrentHashMap<String, UserNetFlow> getUserFlowMap() {
    	return netflowCollector.getUserFlowMap();
    }
    
    public static List<SlowKey> getSlowKey() {
    	return slowKeyCollector.getSlowKeys();
    }
    
  
	public static class AccessStatInfo  {
		
		private String key;
	    private int currentIndex;
	    private AtomicInteger[] procTimes = null;
	    private AtomicInteger[] totalCounter = null;
	    private AtomicInteger[] slowCounter = null;
	    private AtomicLong[] netInBytes = null;
	    private AtomicLong[] netOutBytes = null;
	    
	    private int length;
	    
	    public AccessStatInfo(String key, long currentTimeMillis) {
	        this(key, currentTimeMillis, STATISTIC_PEROID * 2);
	    }
	    
	    public AccessStatInfo(String key, long currentTimeMillis, int length) {
	    	this.key = key;
	        this.procTimes = initAtomicIntegerArr(length);
	        this.totalCounter = initAtomicIntegerArr(length);
	        this.slowCounter = initAtomicIntegerArr(length);
	        this.netInBytes = initAtomicLongArr(length);
	        this.netOutBytes = initAtomicLongArr(length);        
	        this.length = length;
	        this.currentIndex = getIndex(currentTimeMillis, length);
	    }
	    
	    private AtomicInteger[] initAtomicIntegerArr(int size) {
	        AtomicInteger[] arrs = new AtomicInteger[size];
	        for (int i = 0; i < arrs.length; i++) {
	            arrs[i] = new AtomicInteger(0);
	        }
	        return arrs;
	    }
	    
	    private AtomicLong[] initAtomicLongArr(int size) {
	    	AtomicLong[] arrs = new AtomicLong[size];
	        for (int i = 0; i < arrs.length; i++) {
	            arrs[i] = new AtomicLong(0);
	        }
	        return arrs;
	    }
	    
	    private int getIndex(long currentTimeMillis, int periodSecond) {
	    	int index = (int) ((currentTimeMillis / 1000) % periodSecond);
	        return index;
	    }
	    
	    /**
	     * 收集
	     * @param currentTimeMillis
	     * @param procTimeMills
	     */
	    public void collect(long currentTimeMillis, long procTimeMills, int requestSize, int responseSize) {
	    	
	        int tempIndex = getIndex(currentTimeMillis, length);
	        if (currentIndex != tempIndex) {
	            synchronized (this) {
	                // 这一秒的第一条统计，把对应的存储位的数据置0
	                if (currentIndex != tempIndex) {
	                    reset(tempIndex);
	                    currentIndex = tempIndex;
	                }
	            }
	        }
	        
	        procTimes[currentIndex].addAndGet((int) procTimeMills);
	        totalCounter[currentIndex].incrementAndGet();
	        netInBytes[currentIndex].addAndGet( requestSize );
	        netOutBytes[currentIndex].addAndGet( responseSize );
	        
	        if (procTimeMills >= SLOW_COST) {
	            slowCounter[currentIndex].incrementAndGet();
	        }	        
	    }
	    
		/**
		 * 计算结果
		 * @param currentTimeMillis
		 * @param peroidSecond
		 */
		public AccessStatInfoResult calculate(long currentTimeMillis, int peroidSecond) {

			AccessStatInfoResult result = new AccessStatInfoResult();
			result.key = this.key;
			result.created = currentTimeMillis;

			long currentTimeSecond = currentTimeMillis / 1000;
			currentTimeSecond--; // 当前这秒还没完全结束，因此数据不全，统计从上一秒开始，往前推移peroidSecond

			int startIndex = getIndex(currentTimeSecond * 1000, length);
			for (int i = 0; i < peroidSecond; i++) {
				int currentIndex = (startIndex - i + length) % length;
				result.totalCount += totalCounter[currentIndex].get();
				result.slowCount += slowCounter[currentIndex].get();
				result.procTime += procTimes[currentIndex].get();

				if (totalCounter[currentIndex].get() > result.maxCount) {
					result.maxCount = totalCounter[currentIndex].get();
				} else if (totalCounter[currentIndex].get() < result.minCount || result.minCount == -1) {
					result.minCount = totalCounter[currentIndex].get();
				}
				
				// 计算 NetIN/NetOut 流量
				result.netInBytes[0] += netInBytes[currentIndex].get();
				result.netOutBytes[0] += netOutBytes[currentIndex].get();
				
				//max min net/in bytes
				if (netInBytes[currentIndex].get() > result.netInBytes[1]) {
					result.netInBytes[1] = netInBytes[currentIndex].get();
				} else if (netInBytes[currentIndex].get() < result.netInBytes[2] || result.netInBytes[2] == -1) {
					result.netInBytes[2] = netInBytes[currentIndex].get();
				}
				
				// max min net/out bytes
				if (netOutBytes[currentIndex].get() > result.netOutBytes[1]) {
					result.netOutBytes[1] = netOutBytes[currentIndex].get();
				} else if (netOutBytes[currentIndex].get() < result.netOutBytes[2] || result.netOutBytes[2] == -1) {
					result.netOutBytes[2] = netOutBytes[currentIndex].get();
				}
				
			}			
			result.avgCount = ( result.totalCount / peroidSecond);
			
			// avg net in/out bytes
			result.netInBytes[3] = ( result.netInBytes[0] / peroidSecond );
			result.netOutBytes[3] =  ( result.netOutBytes[0] / peroidSecond );

			return result;
		}

		private void clear(long currentTimeMillis, int peroidSecond) {
			long currentTimeSecond = currentTimeMillis / 1000;
			currentTimeSecond--; // 当前这秒还没完全结束，因此数据不全，统计从上一秒开始，往前推移peroidSecond

			int startIndex = getIndex(currentTimeSecond * 1000, length);
			for (int i = 0; i < peroidSecond; i++) {
				int currentIndex = (startIndex - i + length) % length;
				reset(currentIndex);
			}
		}

		private void reset(int index) {
			procTimes[index].set(0);
			totalCounter[index].set(0);
			slowCounter[index].set(0);
			netInBytes[index].set(0);
			netOutBytes[index].set(0);
		}    
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
	
}
