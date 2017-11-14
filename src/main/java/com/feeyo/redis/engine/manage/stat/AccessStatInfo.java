package com.feeyo.redis.engine.manage.stat;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.redis.engine.manage.stat.StatUtil.AccessStatInfoResult;

public class AccessStatInfo  {
	public static final int SLOW_COST = 50; 			  			// 50秒	
	public static final int STATISTIC_PEROID = 30; 		  			// 30秒
	
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
        
        //当前时刻记录响应速度慢的条数
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

	public void clear(long currentTimeMillis, int peroidSecond) {
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