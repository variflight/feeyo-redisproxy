package com.feeyo.net.nio;

import java.util.Map;

import com.feeyo.redis.config.NetFlowCfg;


/**
 * 流量监控
 *
 */
public class NetFlowMonitor {
	
  	private volatile Map<String, NetFlowCfg> netflowCfgMap;
  	
	public void setCfgs(Map<String, NetFlowCfg> cfgs) {
		this.netflowCfgMap = cfgs;
	}
	
	//
	public boolean pool(String user, long length) {
		
		if ( netflowCfgMap == null || netflowCfgMap.isEmpty() ) {
			return false;
		}
		
		//
		NetFlowCfg cfg = netflowCfgMap.get(user);
		if ( cfg != null && cfg.isControl() ) {
			//
			return cfg.pool( length );
		}
		
		return false;
	}
   
}