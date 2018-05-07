package com.feeyo.kafka.config;

/**
 * offset备份地址配置
 * @author yangtao
 *
 */
public class OffsetManageCfg {
	
	private final String server;
	private final String path;

	public OffsetManageCfg(String server, String path) {
		this.server = server;
		this.path = path;
	}

	public String getServer() {
		return server;
	}

	public String getPath() {
		return path;
	}
}
