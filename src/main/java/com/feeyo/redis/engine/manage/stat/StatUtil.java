package com.feeyo.redis.engine.manage.stat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
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

import com.feeyo.redis.engine.manage.stat.KeyUnit.KeyType;
import com.feeyo.redis.net.backend.callback.TopHundredCallback;
import com.feeyo.redis.net.backend.pool.PhysicalNode;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.handler.DefaultCommandHandler;
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
	private final static int LIMIT_HANDLE_KEY = 500;
	public static final int BIGKEY_SIZE = 1024 * 256;  				// 大于 256K
	
	public static final int SLOW_COST = 50; 			  			// 50秒	
	public static final int STATISTIC_PEROID = 30; 		  			// 30秒

	// COMMAND、KEY
	private static ConcurrentHashMap<String, Command> commandStats = new ConcurrentHashMap<String, Command>();
	private static ConcurrentHashMap<String, BigKey> bigkeyStats = new ConcurrentHashMap<String, BigKey>();
	private static ConcurrentHashMap<String, UserNetIo> userNetIoStats = new ConcurrentHashMap<String, UserNetIo>();
	private static ConcurrentHashMap<String, AtomicLong> procTimeMillsDistribution = new ConcurrentHashMap<String, AtomicLong>();
	
	// ACCESS
	private static ConcurrentHashMap<String, AccessStatInfo> accessStats = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, AccessStatInfoResult> totalResults = new ConcurrentHashMap<>();
	private static List<KeyUnit> keyList = Collections.synchronizedList(new ArrayList<KeyUnit>(LIMIT_HANDLE_KEY));
	private static TopHundredSet topHundredSet = new TopHundredSet();
	
	public static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	public static ScheduledFuture<?> scheduledFuture;
	
	public static long zeroTimeMillis = 0;
	
	static {
		scheduledFuture = executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {		
				//记录top 100
				final List<KeyUnit> keyListCopy = Collections.synchronizedList(new ArrayList<KeyUnit>(keyList));
				keyList.clear();
				if(!keyListCopy.isEmpty())
					NetSystem.getInstance().getBusinessExecutor().execute(new CalculatorTopHundredThread(keyListCopy));
				
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
						
						long sum = 0;
						Set<Entry<String, Command>> entrys = StatUtil.getCommandStats();
						for (Entry<String, Command> entry : entrys) {	
							Long count = entry.getValue().count.get();			
							if ( count != null )
								sum += count;
						}
						
						LOGGER.info("Through cmd count:" + sum);
						
						commandStats.clear();
						bigkeyStats.clear();
						procTimeMillsDistribution.clear();
						userNetIoStats.clear();
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
			final int requestSize, final int responseSize, final int procTimeMills, final boolean isCommandOnly, final TopHundredCollectMsg collectMsg) {
		
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
		        
				UserNetIo userNetIo = userNetIoStats.get(password);
				if ( userNetIo == null ) {
					userNetIo = new UserNetIo();
					userNetIo.user = password;
					userNetIoStats.put(password, userNetIo);
				}
				userNetIo.netIn.addAndGet(requestSize);
				userNetIo.netOut.addAndGet(responseSize);
				
				
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
				
				if(!collectMsg.isWrongType()) {
					String keyStr = new String(key);
					KeyUnit keyUnit = new KeyUnit(cmd, keyStr, TopHundredProcessUtil.transformCommandToType(cmd),collectMsg);
					packageKeys2Deal(keyUnit);
				}
			}
		});
	}
	
	private static void packageKeys2Deal(KeyUnit keyUnit) {
		synchronized (keyList) {
			keyList.add(keyUnit);
			if(LIMIT_HANDLE_KEY == keyList.size()) {
				final List<KeyUnit> keyListCopy = Collections.synchronizedList(new ArrayList<KeyUnit>(keyList));
				keyList.clear();
				NetSystem.getInstance().getBusinessExecutor().execute(new CalculatorTopHundredThread(keyListCopy));
			}
		}
	}
	
	public static class CalculatorTopHundredThread implements Runnable{
		List<KeyUnit> keyUnits;
		
		public CalculatorTopHundredThread(List<KeyUnit> keyUnits) {
			this.keyUnits = keyUnits;
		}

		@Override
		public void run() {
			handlKeysTopHundred();
		}
		
		private void handlKeysTopHundred() {
			for(KeyUnit keyUnit : keyUnits) {
				synchronized (topHundredSet) {
					final KeyUnit keyUnitCopy = keyUnit;
					final String key = keyUnitCopy.getKey();
					final KeyType type = keyUnitCopy.getType();
					if(topHundredSet.contains(key)) {
						if(TopHundredProcessUtil.isAddCommand(keyUnit.getCmd())) {
							KeyUnit keyUnit2 = topHundredSet.find(key);
							if(keyUnit2 != null) {
								keyUnit2.getLength().incrementAndGet();
								return;
							}
						}
					}
					//启动线程处理单个Key
					NetSystem.getInstance().getBusinessExecutor().submit(new Runnable() {
						public void run() {
							PhysicalNode physicalNode = keyUnitCopy.getCollectMsg().getPhysicalNode();
							ByteBuffer buffer = TopHundredProcessUtil.getRequestByteBuffer(key,type);
							//埋点
							RedisFrontConnection frontCon = new RedisFrontConnection(null);
							frontCon.getSession().setRequestCmd(keyUnitCopy.getCmd());
							frontCon.getSession().setRequestKey(key.getBytes());
							try {
								new DefaultCommandHandler(frontCon).writeToCustomerBackend(physicalNode, buffer, new TopHundredCallback());
							} catch (IOException e) {
								return;
							}
						}
					});
				}
			}
		}
	
	}
	
    private static AccessStatInfo getAccessStatInfo(String key, long currentTime) {
        AccessStatInfo item = accessStats.get(key);
        if (item == null) {
            accessStats.putIfAbsent(key, new AccessStatInfo(key, currentTime));
            item = accessStats.get(key);
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
    
    public static TopHundredSet getTopHundredSet() {
    	return topHundredSet;
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
    
    public static Set<Entry<String, UserNetIo>> getUserNetIoStats() {
    	return userNetIoStats.entrySet();
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
	
	public static class UserNetIo {
		public String user;
		public AtomicLong netIn = new AtomicLong(0);
		public AtomicLong netOut = new AtomicLong(0);
	}
	
}
