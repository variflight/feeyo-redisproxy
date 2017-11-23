package com.feeyo.redis.engine.codec;

public enum RedisRequestType {
	
	DEFAULT("DEFAULT"),
	PIPELINE("PIPELINE"),
	MGET("MGET"),
	MSET("MSET"),
	MEXISTS("EXISTS"),
	DEL_MULTIKEY("MULTI_DEL");
	
	private final String cmd;
	
	private RedisRequestType(String cmd) {
		this.cmd = cmd;
	}
	
	public String getCmd() {
		return this.cmd;
	}
	
}
