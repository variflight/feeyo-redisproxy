package com.feeyo.redis.engine.codec;

import com.feeyo.redis.net.front.handler.CommandParse;

public class RedisRequestPolicy {

	private byte typePolicy;
	private byte handlePolicy;
	private byte rwPolicy = -1;
	
	public RedisRequestPolicy(byte type, byte level, byte rw) {
		super();
		this.typePolicy = type;
		this.handlePolicy = level;
		this.rwPolicy = rw;
	}

	public int getHandlePolicy() {
		return handlePolicy;
	}

	public int getTypePolicy() {
		return typePolicy;
	}
	
	public byte getRwPolicy() {
		return rwPolicy;
	}
	
	public boolean isRead() {
		 return rwPolicy == CommandParse.READ_CMD;
	}
	
	public boolean isAutoResp() {
		return handlePolicy == CommandParse.AUTO_RESP_CMD;
	}
	
}