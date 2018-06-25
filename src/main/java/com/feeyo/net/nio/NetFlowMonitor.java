package com.feeyo.net.nio;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.config.NetFlowCfg;


/**
 * 流量监控
 *
 */
public class NetFlowMonitor {
	
  	private volatile Map<String, NetFlowControl> ctrlMap = null;
  	
	public synchronized void setCfgs(Map<String, NetFlowCfg> cfgMap) {
		
		if ( cfgMap != null ) {
			
			Map<String, NetFlowControl> tmpCtrlMap = new HashMap<String, NetFlowControl>();
			
			for(Map.Entry<String, NetFlowCfg>  entry : cfgMap.entrySet() ) {
				String pwd = entry.getKey();
				NetFlowCfg cfg = entry.getValue();
				
				if ( cfg != null && cfg.isControl() ) {
					NetFlowControl  ctrl = new NetFlowControl( cfg );
					tmpCtrlMap.put(pwd, ctrl);
				}
				
			}
			
			this.ctrlMap = tmpCtrlMap;
		}
	}
	
	//
	public boolean pool(String user, long length) {
		
		if ( ctrlMap == null || ctrlMap.isEmpty() ) {
			return false;
		}
		
		//
		NetFlowControl control = ctrlMap.get(user);
		if ( control != null ) {
			return control.pool( length );
		}
		
		return false;
	}
	
	
	//
	class NetFlowControl {
		
		private int perSecondMaxSize;
		private int requestMaxSize;
		//
		private int currentIndex;
		private AtomicLong[] sencondSizes;
		
		public NetFlowControl(NetFlowCfg cfg) {
			
			this.perSecondMaxSize = cfg.getPerSecondMaxSize();
			this.requestMaxSize = cfg.getRequestMaxSize();
			
			//
			this.sencondSizes = new AtomicLong[60];
			for (int i = 0; i < sencondSizes.length; i++) {
				this.sencondSizes[i] = new AtomicLong( perSecondMaxSize );
			}
		}
		
		/**
		 * @param length
		 * @return 是否超出流量
		 */
		public boolean pool(long length) {
			
			if ( length > requestMaxSize) {
				return true;
			}

			if (this.perSecondMaxSize > 0 && length > 0) {

				long currentTimeMillis = TimeUtil.currentTimeMillis();
				// long currentTimeMillis = System.currentTimeMillis();

				int tempIndex = (int) ((currentTimeMillis / 1000) % 60);
				if (currentIndex != tempIndex) {
					synchronized (this) {
						// 这一秒的第一条统计，把对应的存储位的数据置是 max
						if (currentIndex != tempIndex) {
							
							// reset
							sencondSizes[tempIndex].set(perSecondMaxSize);
							currentIndex = tempIndex;
						}
					}
				}

				return decrement(sencondSizes[currentIndex], length) <= 0;
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
		
	}
   
}