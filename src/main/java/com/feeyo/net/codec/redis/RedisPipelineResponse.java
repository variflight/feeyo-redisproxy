package com.feeyo.net.codec.redis;

//应答
public class RedisPipelineResponse {
	
	public static final byte ERR = 0;	// 不全
	public static final byte OK = 1;
	
	private byte status;
	private int count;
	private byte[][] resps;
	
	public RedisPipelineResponse (byte status, int count, byte[][] resps) {
		this.status = status;
		this.count = count;
		this.resps = resps;
	}
	
	public int getCount() {
		return count;
	}
	
	public byte[][] getResps() {
		return resps;
	}
	
	public boolean isOK () {
		return status == OK;
	}
}

