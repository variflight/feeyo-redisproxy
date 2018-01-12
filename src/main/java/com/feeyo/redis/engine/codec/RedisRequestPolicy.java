package com.feeyo.redis.engine.codec;

import com.feeyo.redis.net.front.handler.CommandParse;

public class RedisRequestPolicy {

	private byte categroy;				// 类别 
	private byte handleType;			// 处理类型
	
	private byte rw = -1;
	
	public RedisRequestPolicy(byte category, byte type, byte rw) {
		super();
		this.categroy = category;
		this.handleType = type;
		this.rw = rw;
	}

	public byte getCategory() {
		return categroy;
	}
	
	public byte getHandleType() {
		return handleType;
	}

	public boolean isRead() {
		 return rw == CommandParse.READ_CMD;
	}
	
	public boolean isNotThrough() {
		return handleType == CommandParse.NO_THROUGH_CMD;
	}
	
}