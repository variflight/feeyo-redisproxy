package com.feeyo.redis.net.front.route;

import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.net.front.handler.segment.Segment;

import java.util.List;

/**
 * 
 * @author yangtao
 *
 */
public class RouteResult {
	 

	private final RedisRequestType requestType;
	private final List<RedisRequest> requests;
	private final List<RouteResultNode> nodes;				// 封装后的路由请求，包含路由到的节点和 分组后的请求 index 集合
	
	
	private List<Integer> noThroughIndexs;				// 需要自动应答的 request index 集合
	private List<Segment> segments;
    
	public RouteResult(RedisRequestType requestType, List<RedisRequest> requests, List<RouteResultNode> nodes) {
		
		this.requestType = requestType;
		this.requests = requests;
		
		this.nodes = nodes;
	}

	public RedisRequestType getRequestType() {
		return requestType;
	}
	
	public List<RedisRequest> getRequests() {
		return requests;
	}

	
	public List<RouteResultNode> getRouteResultNodes() {
		return nodes;
	}
	
	public List<Integer> getNoThroughtIndexs() {
		return noThroughIndexs;
	}
	
	public void setNoThroughtIndexs(List<Integer> noThroughIndexs) {
		this.noThroughIndexs = noThroughIndexs;
	}

	public void setSegments(List<Segment> segments) {
		this.segments = segments;
	}

	public  List<Segment> getSegments() {
		return segments;
	}

	// 请求数
	public int getRequestCount() {
		return  requests.size();				
	}

	// 透传数
	public int getThroughtCount() {
		return requests.size() - noThroughIndexs.size();	
	}

	// 不透传数
	public int getNoThroughtCount() {				
		return  noThroughIndexs.size();		
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
	    noThroughIndexs.clear();
	    nodes.clear();
	}
}