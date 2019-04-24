package com.feeyo.redis.config;

public class NetFlowCfg {

	private final String password;
	private final int perSecondMaxSize;
	private final int requestMaxSize;
	private boolean isControl;

	public NetFlowCfg(String password, int perSecondMaxSize, int requestMaxSize, boolean isControl) {
		this.password = password;
		this.perSecondMaxSize = perSecondMaxSize;
		this.requestMaxSize = requestMaxSize;
		this.isControl = isControl;

	}

	public String getPassword() {
		return password;
	}

	public int getPerSecondMaxSize() {
		return perSecondMaxSize;
	}

	public int getRequestMaxSize() {
		return requestMaxSize;
	}

	public boolean isControl() {
		return isControl;
	}

}
