package com.feeyo.redis.engine.codec;

import com.feeyo.redis.net.front.handler.CommandParse;

public class RedisRequestPolicy {

	private int level;
	private byte rw = -1;
	private byte type = -1;
	
	public RedisRequestPolicy(int level, byte rw, byte type) {
		super();
		this.level = level;
		this.rw = rw;
		this.type = type;
	}

	public int getLevel() {
		return level;
	}

	public byte getRw() {
		return rw;
	}
	
	public byte getType() {
		return type;
	}
	
	public boolean isRead() {
		 return (rw == CommandParse.READ_CMD) ? true : false;
	}
	
}