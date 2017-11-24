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
	
	private List<Integer> autoResponseIndexs;				// 需要自动应答的 request index 集合
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
	
	public List<Integer> getAutoResponseIndexs() {
		return autoResponseIndexs;
	}
	
	public void setAutoResponseIndexs(List<Integer> autoResponseIndexs) {
		this.autoResponseIndexs = autoResponseIndexs;
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
	    autoResponseIndexs.clear();
	    nodes.clear();
	}
}