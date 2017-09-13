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
	
	private final RedisRequestType requestType;
	
	private final List<RedisRequest> requests;
	private final List<RedisRequestPolicy> requestPolicys;
	
	private final List<Integer> autoResponseIndexs;			// 需要自动应答的 request index 集合
	private final List<RouteResultNode> nodes;				// 封装后的路由请求，包含路由到的节点和 分组后的请求 index 集合
	
    private final int requestCount;							// 请求数
    private final int transCount;							// 透传数
    private final int autoRespCount;						// 自动应答数
    
    
	public RouteResult(RedisRequestType requestType, List<RedisRequest> requests, List<RedisRequestPolicy> requestPolicys, 
			List<RouteResultNode> nodes, List<Integer> autoResponseIndexs) {
		
		this.requestType = requestType;
		this.requests = requests;
		this.requestPolicys = requestPolicys;
		
		this.nodes = nodes;
		this.autoResponseIndexs = autoResponseIndexs;
		
		this.requestCount = requests.size();
		this.autoRespCount = autoResponseIndexs.size();
		this.transCount =  requestCount - autoRespCount;
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

	public int getRequestCount() {
		return requestCount;
	}

	public int getTransCount() {
		return transCount;
	}

	public int getAutoRespCount() {
		return autoRespCount;
	}
	
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