package com.feeyo.redis.nio;

import java.io.File;
import java.io.IOException;

/**
 * 系统基础配置项
 */
public final class SystemConfig {
	
	public static final int DEFAULT_PROCESSORS = Runtime.getRuntime().availableProcessors();
	public static final String FEEYO_HOME = "FEEYO_HOME";
	
	private int frontIdleTimeout    = 5 * 60 * 1000;	// 单位毫秒
	private int backendIdleTimeout	= 30 * 60 * 1000;	
	
	private int frontSocketSoRcvbuf = 1024 * 1024;
	private int frontSocketSoSndbuf = 4 * 1024 * 1024;
	
	private int backSocketSoRcvbuf  = 4 * 1024 * 1024;	
	
	private int backSocketSoSndbuf = 1024 * 1024;
	private int frontSocketNoDelay = 1; 				// 0=false
	private int backSocketNoDelay  = 1; 				// 1=true
	
	public SystemConfig() {

	}

	public static String getHomePath() {
		String home = System.getProperty(SystemConfig.FEEYO_HOME);
		if (home != null) {
			if (home.endsWith(File.pathSeparator)) {
				home = home.substring(0, home.length() - 1);
				System.setProperty(SystemConfig.FEEYO_HOME, home);
			}
		}

		// HOME为空，默认尝试设置为当前目录或上级目录。BEN
		if (home == null) {
			try {
				String path = new File("..").getCanonicalPath().replaceAll("\\\\", "/");
				File conf = new File(path + "/conf");
				if (conf.exists() && conf.isDirectory()) {
					home = path;
				} else {
					path = new File(".").getCanonicalPath().replaceAll("\\\\", "/");
					conf = new File(path + "/conf");
					if (conf.exists() && conf.isDirectory()) {
						home = path;
					}
				}

				if (home != null) {
					System.setProperty(SystemConfig.FEEYO_HOME, home);
				}
			} catch (IOException e) {
				// 如出错，则忽略。
			}
		}
		return home;
	}


	public int getFrontIdleTimeout() {
		return frontIdleTimeout;
	}

	public void setFrontIdleTimeout(int frontIdleTimeout) {
		this.frontIdleTimeout = frontIdleTimeout;
	}

	public int getBackendIdleTimeout() {
		return backendIdleTimeout;
	}

	public void setBackendIdleTimeout(int backendIdleTimeout) {
		this.backendIdleTimeout = backendIdleTimeout;
	}

	public int getFrontsocketsorcvbuf() {
		return frontSocketSoRcvbuf;
	}

	public int getFrontsocketsosndbuf() {
		return frontSocketSoSndbuf;
	}

	public int getBacksocketsorcvbuf() {
		return backSocketSoRcvbuf;
	}

	public int getBacksocketsosndbuf() {
		return backSocketSoSndbuf;
	}

	
	public int getFrontSocketSoRcvbuf() {
		return frontSocketSoRcvbuf;
	}

	public void setFrontSocketSoRcvbuf(int frontSocketSoRcvbuf) {
		this.frontSocketSoRcvbuf = frontSocketSoRcvbuf;
	}

	public int getFrontSocketSoSndbuf() {
		return frontSocketSoSndbuf;
	}

	public void setFrontSocketSoSndbuf(int frontSocketSoSndbuf) {
		this.frontSocketSoSndbuf = frontSocketSoSndbuf;
	}

	public int getBackSocketSoRcvbuf() {
		return backSocketSoRcvbuf;
	}

	public void setBackSocketSoRcvbuf(int backSocketSoRcvbuf) {
		this.backSocketSoRcvbuf = backSocketSoRcvbuf;
	}

	public int getBackSocketSoSndbuf() {
		return backSocketSoSndbuf;
	}

	public void setBackSocketSoSndbuf(int backSocketSoSndbuf) {
		this.backSocketSoSndbuf = backSocketSoSndbuf;
	}

	public int getFrontSocketNoDelay() {
		return frontSocketNoDelay;
	}

	public void setFrontSocketNoDelay(int frontSocketNoDelay) {
		this.frontSocketNoDelay = frontSocketNoDelay;
	}

	public int getBackSocketNoDelay() {
		return backSocketNoDelay;
	}

	public void setBackSocketNoDelay(int backSocketNoDelay) {
		this.backSocketNoDelay = backSocketNoDelay;
	}
}