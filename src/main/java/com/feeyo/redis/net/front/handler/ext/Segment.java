package com.feeyo.redis.net.front.handler.ext;


//记录Pipeline 方式下命令的请求类型与命令对应的请求索引（拆分mset、mget后的索引）
public class Segment {
	
	private final SegmentType type;
	private final int[] indexs;

	public Segment(SegmentType type, int[] indexs) {
		this.type = type;
		this.indexs = indexs;
	}

	public int[] getIndexs() {
		return indexs;
	}

	public SegmentType getType() {
		return type;
	}
}
