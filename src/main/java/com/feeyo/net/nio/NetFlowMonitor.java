package com.feeyo.net.nio;

import java.util.HashMap;
import java.util.Map;

import com.feeyo.redis.config.ConfigLoader;
import com.feeyo.redis.config.NetFlowCfg;


/**
 * 流量监控
 *
 */
public class NetFlowMonitor {
	
  	private Map<String, NetFlowCfg> netFlowMap;
	private volatile boolean isOpen;
  	
	public NetFlowMonitor() {
		try {
			this.netFlowMap = ConfigLoader.loadNetFlowMap(ConfigLoader.buidCfgAbsPathFor("netflow.xml"));
		} catch (Exception e) {
			this.netFlowMap = new HashMap<String, NetFlowCfg>();
		}
		if (netFlowMap == null || netFlowMap.isEmpty()) {
			isOpen = false;
		} else {
			isOpen = true;
		}
	}
	
	/**
	 * @param user
	 * @param length
	 * @return 是否超出流量
	 */
	public boolean pool(String user, long length) {
		if (!isOpen) {
			return false;
		}
		
		NetFlowCfg netFlowCfg = netFlowMap.get(user);
		
		if (netFlowCfg != null) {
			return netFlowCfg.pool(length);
		}
		
		return false;
	}

	public byte[] reload() {
		try {
			this.netFlowMap = ConfigLoader.loadNetFlowMap(ConfigLoader.buidCfgAbsPathFor("netflow.xml"));
			if (netFlowMap == null || netFlowMap.isEmpty()) {
				isOpen = false;
			} else {
				isOpen = true;
			}
		} catch (Exception e) {
			// TODO: handle exception
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ").append(e.getMessage()).append("\r\n");
			return sb.toString().getBytes();
		}
		return "+OK\r\n".getBytes();
	}
    
}
