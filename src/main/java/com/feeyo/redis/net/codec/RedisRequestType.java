package com.feeyo.redis.net.codec;

public enum RedisRequestType {
	
	DEFAULT("DEFAULT"),
	BLOCK("BLOCK"),
	PIPELINE("PIPELINE"),
	MGET("MGET"),
	MSET("MSET"),
	MEXISTS("EXISTS"),
	DEL_MULTIKEY("MULTI_DEL"),
	
	KAFKA("KAFKA");
	
	private final String cmd;
	
	private RedisRequestType(String cmd) {
		this.cmd = cmd;
	}
	
	public String getCmd() {
		return this.cmd;
	}
	
}
