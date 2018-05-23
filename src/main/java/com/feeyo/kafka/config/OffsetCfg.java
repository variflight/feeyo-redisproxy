package com.feeyo.kafka.config;

/**
 * Offset 基础管理配置
 * 
 * @author yangtao
 *
 */
public class OffsetCfg {
	
	private final String zkServerIp;
	private final String path;
	
	private final String localIp;

	public OffsetCfg(String zkServerIp, String path, String localIp) {
		this.zkServerIp = zkServerIp;
		this.path = path;
		this.localIp = localIp;
	}

	public String getZkServerIp() {
		return zkServerIp;
	}

	public String getPath() {
		return path;
	}

	public String getLocalIp() {
		return localIp;
	}
	
}
