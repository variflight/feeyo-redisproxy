package com.feeyo.kafka.config;

/**
 * offset备份地址配置
 * @author yangtao
 *
 */
public class OffsetManageCfg {
	
	private final String server;
	private final String offsetPath;
	private final String runningPath;
	private final String clusterPath;

	public OffsetManageCfg(String server, String offsetPath, String runningPath, String clusterPath) {
		this.server = server;
		this.offsetPath = offsetPath;
		this.runningPath = runningPath;
		this.clusterPath = clusterPath;
	}

	public String getServer() {
		return server;
	}

	public String getOffsetPath() {
		return offsetPath;
	}

	public String getRunningPath() {
		return runningPath;
	}

	public String getClusterPath() {
		return clusterPath;
	}
}
