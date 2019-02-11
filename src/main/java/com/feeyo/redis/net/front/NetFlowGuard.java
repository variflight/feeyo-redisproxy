package com.feeyo.redis.net.front;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.config.NetFlowCfg;


/*
 * 网络流防护 
 */
public class NetFlowGuard {
	
  	private volatile Map<String, Guard> guardMap = null;
  	
  	//
	public synchronized void setCfgs(Map<String, NetFlowCfg> cfgMap) {
		
		if ( cfgMap != null ) {
			
			Map<String, Guard> tmpGuardMap = new HashMap<String, Guard>();
			
			for(Map.Entry<String, NetFlowCfg>  entry : cfgMap.entrySet() ) {
				String pwd = entry.getKey();
				NetFlowCfg cfg = entry.getValue();
				
				if ( cfg != null && cfg.isControl() ) {
					Guard  guard = new Guard( cfg.getPerSecondMaxSize(), cfg.getRequestMaxSize() );
					tmpGuardMap.put(pwd, guard);
				}
				
			}
			
			this.guardMap = tmpGuardMap;
		}
	}
	
	//
	public boolean consumeBytes(String user, long numBytes) {
		
		if ( guardMap == null || guardMap.isEmpty() ) {
			return false;
		}
		
		//
		Guard guard = guardMap.get(user);
		if ( guard != null ) {
			return guard.consumeBytes( numBytes );
		}
		
		return false;
	}
	
	public Map<String, Guard> getGuardMap() {
		return guardMap;
	}


	//
	public class Guard {
		
		private int perSecondMaxSize;
		private int requestMaxSize;
		
		//
		private AtomicLong[] availableSizes;
		private int currentIndex;
		
		public Guard(int perSecondMaxSize, int  requestMaxSize) {
			
			this.perSecondMaxSize = perSecondMaxSize;
			this.requestMaxSize = requestMaxSize;
			
			//
			this.availableSizes = new AtomicLong[60];
			for (int i = 0; i < availableSizes.length; i++) {
				this.availableSizes[i] = new AtomicLong( perSecondMaxSize );
			}
		}
		
		// 检测是否超出流量
		public boolean consumeBytes(long numBytes) {
			
			if ( numBytes > requestMaxSize) {
				return true;
			}

			if (this.perSecondMaxSize > 0 && numBytes > 0) {

				int tempIndex = TimeUtil.currentTimeSecondIndex();
				if (currentIndex != tempIndex) {
					synchronized (this) {
						// 这一秒的第一条统计，把对应的存储位的数据置是 max
						if (currentIndex != tempIndex) {
							
							// reset
							availableSizes[tempIndex].set(perSecondMaxSize);
							currentIndex = tempIndex;
						}
					}
				}

				return decrement(availableSizes[currentIndex], numBytes) <= 0;
			}

			return true;
		}
		
		private final long decrement(AtomicLong atomicLong, long delta) {
			for (;;) {
				long current = atomicLong.get();
				long next = current - delta;
				if (atomicLong.compareAndSet(current, next))
					return next;
			}
		}

		public int getPerSecondMaxSize() {
			return perSecondMaxSize;
		}

		public int getRequestMaxSize() {
			return requestMaxSize;
		}
		
		public String getHistogram() {
			if ( availableSizes != null && availableSizes.length == 60 ) {
			    StringBuilder buf = new StringBuilder();
		        buf.append('[');
		        for (int i = 0; i < availableSizes.length; i++) {
		            if (i != 0) {
		                buf.append(", ");
		            }
		            buf.append( perSecondMaxSize - availableSizes[i].get() );
		        }
		        buf.append(']');
		        return buf.toString();
			}
			return "";
		}
		
	}
   
}