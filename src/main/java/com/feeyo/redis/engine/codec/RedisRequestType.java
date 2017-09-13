package com.feeyo.redis.engine.codec;

public enum RedisRequestType {
	
	DEFAULT,
	
	PIPELINE,
	MGET,
	MSET,
	DEL_MULTIKEY
	
}
