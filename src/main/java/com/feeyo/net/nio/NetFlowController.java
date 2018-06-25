package com.feeyo.net.nio;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.config.NetFlowCfg;


/**
 * 流量监控, bandwidth limitation
 *
 */
public class NetFlowController {
	
  	private volatile Map<String, Ctrl> ctrlMap = null;
  	
  	//
	public synchronized void setCfgs(Map<String, NetFlowCfg> cfgMap) {
		
		if ( cfgMap != null ) {
			
			Map<String, Ctrl> tmpCtrlMap = new HashMap<String, Ctrl>();
			
			for(Map.Entry<String, NetFlowCfg>  entry : cfgMap.entrySet() ) {
				String pwd = entry.getKey();
				NetFlowCfg cfg = entry.getValue();
				
				if ( cfg != null && cfg.isControl() ) {
					Ctrl  ctrl = new Ctrl( cfg.getPerSecondMaxSize(), cfg.getRequestMaxSize() );
					tmpCtrlMap.put(pwd, ctrl);
				}
				
			}
			
			this.ctrlMap = tmpCtrlMap;
		}
	}
	
	//
	public boolean consumeBytes(String user, long numBytes) {
		
		if ( ctrlMap == null || ctrlMap.isEmpty() ) {
			return false;
		}
		
		//
		Ctrl ctrl = ctrlMap.get(user);
		if ( ctrl != null ) {
			return ctrl.consumeBytes( numBytes );
		}
		
		return false;
	}
	
	public Map<String, Ctrl> getCtrlMap() {
		return ctrlMap;
	}


	//
	public class Ctrl {
		
		private int perSecondMaxSize;
		private int requestMaxSize;
		
		//
		private AtomicLong[] sizes;
		private int currentIndex;
		
		public Ctrl(int perSecondMaxSize, int  requestMaxSize) {
			
			this.perSecondMaxSize = perSecondMaxSize;
			this.requestMaxSize = requestMaxSize;
			
			//
			this.sizes = new AtomicLong[60];
			for (int i = 0; i < sizes.length; i++) {
				this.sizes[i] = new AtomicLong( perSecondMaxSize );
			}
		}
		
		// 检测是否超出流量
		public boolean consumeBytes(long numBytes) {
			
			if ( numBytes > requestMaxSize) {
				return true;
			}

			if (this.perSecondMaxSize > 0 && numBytes > 0) {

				long currentTimeMillis = TimeUtil.currentTimeMillis();
				// long currentTimeMillis = System.currentTimeMillis();

				int tempIndex = (int) ((currentTimeMillis / 1000) % 60);
				if (currentIndex != tempIndex) {
					synchronized (this) {
						// 这一秒的第一条统计，把对应的存储位的数据置是 max
						if (currentIndex != tempIndex) {
							
							// reset
							sizes[tempIndex].set(perSecondMaxSize);
							currentIndex = tempIndex;
						}
					}
				}

				return decrement(sizes[currentIndex], numBytes) <= 0;
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
		
		public long getCurrentUsedSize() {
			return perSecondMaxSize - sizes[currentIndex].get();
		}
		
	}
   
}