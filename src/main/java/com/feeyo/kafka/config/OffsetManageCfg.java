package com.feeyo.kafka.config;

import java.io.File;

/**
 * offset备份地址配置
 * @author yangtao
 *
 */
public class OffsetManageCfg {
	
	private final String server;
	private final String path;
	private final static String offsetNodeName = "offsets";
	private final static String clusterNodeName = "cluster";
	private final static String runningNodeName = "running";

	public OffsetManageCfg(String server, String path) {
		this.server = server;
		this.path = path;
	}

	public String getServer() {
		return server;
	}

	public String getOffsetPath() {
		return path + File.pathSeparator + offsetNodeName;
	}

	public String getRunningPath() {
		return path + File.pathSeparator + runningNodeName;
	}

	public String getClusterPath() {
		return path + File.pathSeparator + clusterNodeName;
	}
}
