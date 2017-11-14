package com.feeyo.redis.net.front.route;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestPolicy;
import com.feeyo.redis.engine.codec.RedisRequestType;

import java.util.List;

/**
 * 
 * @author yangtao
 *
 */
public class RouteResult {
	 
	public enum PipelineCommandType {
		MGET_OP_COMMAND,  //由pipeline中MGET拆分出来的命令标志
		MSET_OP_COMMAND,  //由pipeline中MSET拆分出来的命令标志
		DEFAULT_OP_COMMAND  //pipeline中的其他命令（非批量操作命令）
	}
	
	private final List<RequestIndexCombination> requestIndexCombinations;
	
	private final RedisRequestType requestType;
	
	private final List<RedisRequest> requests;
	private final List<RedisRequestPolicy> requestPolicys;
	
	private final List<Integer> autoResponseIndexs;			// 需要自动应答的 request index 集合
	private final List<RouteResultNode> nodes;				// 封装后的路由请求，包含路由到的节点和 分组后的请求 index 集合
    
    
	public RouteResult(RedisRequestType requestType, List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys, 
			List<RouteResultNode> nodes, List<Integer> autoResponseIndexs, List<RequestIndexCombination>  requestIndexCombinations) {
		
		this.requestType = requestType;
		this.requests = requests;
		this.requestPolicys = requestPolicys;
		
		this.nodes = nodes;
		this.autoResponseIndexs = autoResponseIndexs;
		this.requestIndexCombinations = requestIndexCombinations;
	}

	public RedisRequestType getRequestType() {
		return requestType;
	}
	
	public List<RedisRequest> getRequests() {
		return requests;
	}

	public List<RedisRequestPolicy> getRequestPolicys() {
		return requestPolicys;
	}
	
	public List<Integer> getAutoResponseIndexs() {
		return autoResponseIndexs;
	}
	
	public List<RouteResultNode> getRouteResultNodes() {
		return nodes;
	}
	
	public  List<RequestIndexCombination> getRequestIndexCombinations() {
		return requestIndexCombinations;
	}

	// 请求数
	public int getRequestCount() {
		return  requests.size();				
	}

	// 透传数
	public int getTransCount() {
		return requests.size() - autoResponseIndexs.size();	
	}

	// 自动应答数
	public int getAutoRespCount() {				
		return  autoResponseIndexs.size();		
	}
	
	// 请求字节数
	public int getRequestSize() {				
		int size = 0;
		for(RedisRequest req: requests ) {
			size += req.getSize();
		}
		return size;
	}
	
	public void clear() {
	    for (RedisRequest request : requests) {
	        request.clear();
	    }
	    
	    requests.clear();
	    requestPolicys.clear();
	    autoResponseIndexs.clear();
	    nodes.clear();
	}
}