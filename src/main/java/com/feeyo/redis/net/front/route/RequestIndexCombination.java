package com.feeyo.redis.net.front.route;

import com.feeyo.redis.net.front.route.RouteResult.PipelineCommandType;

//记录Pipeline 方式下命令的请求类型与命令对应的请求索引（拆分mset、mget后的索引）
public class RequestIndexCombination {
	
	private final PipelineCommandType type;
	
	private final int[] indexCombinations;

	public RequestIndexCombination(PipelineCommandType type, int[] indexCombinations) {
		this.type = type;
		this.indexCombinations = indexCombinations;
	}

	public int[] getIndexCombinations() {
		return indexCombinations;
	}

	public PipelineCommandType getType() {
		return type;
	}
}
