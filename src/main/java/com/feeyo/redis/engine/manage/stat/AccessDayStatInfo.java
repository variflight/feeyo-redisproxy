package com.feeyo.redis.engine.manage.stat;

import java.util.concurrent.atomic.AtomicInteger;

import com.feeyo.redis.engine.manage.stat.StatUtil.AccessStatInfoResult;

public class AccessDayStatInfo{
	private static final int TWO_STATISTIC_PEROID = 2*30; 		  			// 60秒
	
	private AccessStatInfoResult infoResult;
	
	private AtomicInteger hour = new AtomicInteger(-1);
	private AccessStatInfoResult[] hoursInfoResult = new AccessStatInfoResult[24];
	private AccessHourStatInfo currentHour;
	public AccessDayStatInfo(String key, long currentTimeMillis) {
		this.currentHour = new AccessHourStatInfo(key, currentTimeMillis);
	}
	
	public AccessHourStatInfo getCurrentHour() {
		return currentHour;
	}

	public class AccessHourStatInfo{
		private AtomicInteger minute = new AtomicInteger(-1);
		private AccessStatInfoResult[] minutesInfoResult = new AccessStatInfoResult[60];
		private AccessMinuteStatInfo currentMinute;
		
		public AccessHourStatInfo(String key, long currentTimeMillis) {
			this.currentMinute = new AccessMinuteStatInfo(key, currentTimeMillis);
		}
		
		public AccessMinuteStatInfo getCurrentMinute() {
			return currentMinute;
		}
		
		public AtomicInteger getMinute() {
			return minute;
		}

		public synchronized void addMinuteStatInfo(String key, long currentTimeMillis, int minute){
			currentMinute.calculate(currentTimeMillis, TWO_STATISTIC_PEROID);
			this.minute = new AtomicInteger(minute);
			minutesInfoResult[minute] = currentMinute.getAccessStatInfoResult();
			currentMinute.statInfo.clear(currentTimeMillis, TWO_STATISTIC_PEROID);
		}
		
		public synchronized AccessStatInfoResult mergeAccessStatInfoResultForOneHour() {
			AccessStatInfoResult mergeInfoResult = new AccessStatInfoResult();
			for(int i = 0; i < minute.get()+1; i++){
				if(null == minutesInfoResult[i])
					continue;
				AccessStatInfoResult minuteInfoResult = minutesInfoResult[i];
				mergeTwoAccessStatInfoResult(mergeInfoResult,minuteInfoResult);
			}
			mergeInfoResult.avgCount = mergeInfoResult.totalCount / minute.get()*60;
			mergeInfoResult.netInBytes[3] = mergeInfoResult.netInBytes[0] / minute.get()*60;
			mergeInfoResult.netOutBytes[3] = mergeInfoResult.netOutBytes[0] / minute.get()*60;
			
			return mergeInfoResult;
		}
	}
	
	public class AccessMinuteStatInfo{
		private AccessStatInfoResult infoResult;
		private AccessStatInfo statInfo;
		
		public AccessMinuteStatInfo(String key, long currentTimeMillis) {
			this.statInfo = new AccessStatInfo(key, currentTimeMillis);
		}
		
		public void collect(long currentTimeMillis, long procTimeMills, int requestSize, int responseSize) {
			this.statInfo.collect(currentTimeMillis, procTimeMills, requestSize, responseSize);
		}
		
		public void calculate(long currentTimeMillis, int peroidSecond) {
			this.infoResult = statInfo.calculate(currentTimeMillis, peroidSecond);
		}
		
		public AccessStatInfoResult getAccessStatInfoResult() {
			return infoResult;
		}
		
    }
	 
	public synchronized void addHoursStatInfo(String key, long currentTimeMillis, int hour){
		this.hour = new AtomicInteger(hour);
		hoursInfoResult[hour] = currentHour.mergeAccessStatInfoResultForOneHour();
	}
	
	public synchronized AccessStatInfoResult calculate(long currentTimeMillis) {
		infoResult = mergeAccessStatInfoResultForOneDay();
		infoResult.created = currentTimeMillis;
		return infoResult;
	}
	
	public synchronized void collect(long currentTimeMillis, long procTimeMills, int requestSize, int responseSize) {
		currentHour.getCurrentMinute().collect(currentTimeMillis, procTimeMills, requestSize, responseSize);
	}
	
	private synchronized AccessStatInfoResult mergeAccessStatInfoResultForOneDay() {
		AccessStatInfoResult mergeInfoResult = new AccessStatInfoResult();
		for(int i = 0; i < hour.get()+1; i++){
			if(null == hoursInfoResult[i])
				continue;
			AccessStatInfoResult hourInfoResult = hoursInfoResult[i];
			mergeTwoAccessStatInfoResult(mergeInfoResult,hourInfoResult);
		}
		int secondNum = (hour.get()*3600 + getCurrentHour().minute.get()*60);
		mergeInfoResult.avgCount = mergeInfoResult.totalCount / (hour.get()*3600 + getCurrentHour().minute.get()*60);
		mergeInfoResult.netInBytes[3] = mergeInfoResult.netInBytes[0] / secondNum;
		mergeInfoResult.netOutBytes[3] = mergeInfoResult.netOutBytes[0] / secondNum;
		
		return mergeInfoResult;
	}
	
	public synchronized void mergeTwoAccessStatInfoResult(AccessStatInfoResult disc , AccessStatInfoResult src) {
		if(null != src) {
			disc.key = src.key;
			
			disc.totalCount += src.totalCount;
			disc.slowCount += src.slowCount;
			disc.procTime += src.procTime;
			disc.totalCount += src.totalCount;
			disc.totalCount += src.totalCount;
			disc.totalCount += src.totalCount;
			if(src.maxCount > disc.maxCount)
				disc.maxCount = src.maxCount;
			if(src.minCount < disc.minCount)
				disc.minCount = src.minCount;
			disc.netInBytes[0] += src.netInBytes[0];
			disc.netOutBytes[0] += src.netOutBytes[0];
			if(src.netInBytes[1] > disc.netInBytes[1]) {
				disc.netInBytes[1] = src.netInBytes[1];
			}else if(src.netInBytes[2] < disc.netInBytes[2] || disc.netInBytes[2] == -1) {
				disc.netInBytes[2] = src.netInBytes[2];
			}
			if(src.netOutBytes[1] > disc.netOutBytes[1]) {
				disc.netOutBytes[1] = src.netOutBytes[1];
			}else if(src.netOutBytes[2] < disc.netOutBytes[2] || disc.netOutBytes[2] == -1) {
				disc.netOutBytes[2] = src.netOutBytes[2];
			}
		}
	}
}