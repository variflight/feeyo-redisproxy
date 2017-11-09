package com.feeyo.redis.net.front.route;

import com.feeyo.redis.net.front.route.RouteResult.PIPELINE_COMMAND_TYPE;

//记录Pipeline 方式下命令的请求类型与命令对应的请求索引（拆分mset、mget后的索引）
public class RequestIndexCombination {
	
	private final PIPELINE_COMMAND_TYPE type;
	
	private final int[] indexCombinations;

	public RequestIndexCombination(PIPELINE_COMMAND_TYPE type, int[] indexCombinations) {
		this.type = type;
		this.indexCombinations = indexCombinations;
	}

	public int[] getIndexCombinations() {
		return indexCombinations;
	}

	public PIPELINE_COMMAND_TYPE getType() {
		return type;
	}
}
