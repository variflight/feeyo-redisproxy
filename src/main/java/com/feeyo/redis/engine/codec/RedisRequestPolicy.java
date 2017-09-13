package com.feeyo.redis.engine.codec;

public class RedisRequestPolicy {
	
	// 特殊指令策略
	public static final int UNKNOW_CMD   	= -1;		// 未知指令
	public static final int DISABLED_CMD 	= 0;		// 禁止指令
	public static final int CLUSTER_CMD 	= 1;		// 集群指令
	public static final int NO_CLUSTER_CMD 	= 2;		// 非集群指令
	public static final int MANAGE_CMD 		= 3;		// 管理指令
	public static final int PUBSUB_CMD 		= 4;		// 
	public static final int THROUGH_CMD 	= 5;		// 透传指令

	public static final int AUTO_RESP_CMD	= 7;		// 中间件应答指令
	public static final int MGETSET_CMD		= 9;		// 中间件加强指令
	public static final int DEL_CMD			= 10;		// 中间件加强指令
	
	// RW 指令
	public static final byte WRITE_CMD = 1;
	public static final byte READ_CMD = 2;
	
	
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
		 return (rw == READ_CMD) ? true : false;
	}
	
}