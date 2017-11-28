package com.feeyo.redis.engine.codec;

import com.feeyo.redis.net.front.handler.CommandParse;

public class RedisRequestPolicy {

	private int level;
	private byte rw = -1;
	
	public RedisRequestPolicy(int level, byte rw) {
		super();
		this.level = level;
		this.rw = rw;
	}

	public int getLevel() {
		return level;
	}

	public byte getRw() {
		return rw;
	}
	
	public boolean isRead() {
		 return rw == CommandParse.READ_CMD;
	}
	
	public boolean isAutoResp() {
		return level == CommandParse.AUTO_RESP_CMD;
	}
	
}