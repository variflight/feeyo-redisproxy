package com.feeyo.redis.net.front.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.codec.RedisRequest;
import com.feeyo.redis.engine.codec.RedisRequestEncoderV2;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.virtualmemory.AppendMessageResult;
import com.feeyo.redis.virtualmemory.Message;
import com.feeyo.redis.virtualmemory.PutMessageResult;
import com.feeyo.redis.virtualmemory.Util;

/**
 * 抽象 pipeline 处理
 *
 */
public abstract class AbstractPipelineCommandHandler extends AbstractCommandHandler {
	
	private static Logger LOGGER = LoggerFactory.getLogger( AbstractPipelineCommandHandler.class );
	
	// 应答状态码
	public enum ResponseStatusCode {
		ALL_NODE_COMPLETED,		//所有节点都完成
		THE_NODE_COMPLETED,		//当前节点完成
		INCOMPLETE,				//未完整
		ERROR					//错误
	}

	
	// 应答节点
	public class ResponseNode {
		
		private String address;
		private RouteResultNode node;
		
		private int responseCount = 0;
		private ConcurrentLinkedQueue<PutMessageResult> bufferQueue = new ConcurrentLinkedQueue<PutMessageResult>();
		
		public ResponseNode(RouteResultNode node) {
			super();
			this.node = node;
			this.address = node.getPhysicalNode().getName();
		}
		
		public String getAddress() {
			return address;
		}

		public RouteResultNode getNode() {
			return node;
		}
		
		public void addResponseCount(int count) {
			this.responseCount += count;
		}

		public int getResponseCount() {
			return responseCount;
		}

		public ConcurrentLinkedQueue<PutMessageResult> getBufferQueue() {
			return bufferQueue;
		}
	}
	
	
	public final static String PIPELINE_CMD = "pipeline";
	public final static byte[] PIPELINE_KEY = PIPELINE_CMD.getBytes();
	
	protected RedisRequestEncoderV2 encoder = new RedisRequestEncoderV2();
	
	protected RouteResult rrs;
	
	
	private ConcurrentHashMap<Long, RedisBackendConnection> backendConnections = new ConcurrentHashMap<Long, RedisBackendConnection>();
	
	private ConcurrentHashMap<String, ResponseNode> responseNodeMap =  new ConcurrentHashMap<String, ResponseNode>(); 
	private AtomicInteger allResponseCount = new AtomicInteger(0); 					// 接收到返回数据的条数
	private AtomicBoolean isMarged = new AtomicBoolean(false);

	public AbstractPipelineCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}
	
	@Override
	protected void commonHandle(RouteResult rrs) throws IOException {
		
		this.rrs = rrs;
		this.allResponseCount.set(0);
		this.isMarged.set( false );
		
		this.responseNodeMap.clear();
		
		for(RouteResultNode node: rrs.getRouteResultNodes()) {
			String address = node.getPhysicalNode().getName();
			ResponseNode reponseNode = new ResponseNode( node );
			responseNodeMap.put( address, reponseNode );
		}
	}
	
	
	protected ByteBuffer getRequestBufferByRRN(RouteResultNode rrn) {
		ByteBuffer buffer = null;
		List<RedisRequest> requests = rrs.getRequests();
		
		// 编码
		List<Integer> indexs = rrn.getRequestIndexs();
		if ( indexs.size() == 1 ) {
			buffer = requests.get( indexs.get(0) ).encode();
			
		} else {
			List<RedisRequest> tmpRequests = new ArrayList<RedisRequest>( indexs.size() );
			for(int idx: indexs) {
				tmpRequests.add( requests.get( idx ) );
			}
			buffer = encoder.encode( tmpRequests);
		}	
		return buffer;
	}

	//
	protected synchronized ResponseStatusCode recvResponse(String address, int count, byte[][] resps) {
		
		ResponseNode responseNode = responseNodeMap.get( address );
		
		if ( resps != null && resps.length > 0) {
			for (byte[] resp : resps) {
				
				Message msg = new Message();
				msg.setBody( resp );
				msg.setBodyCRC( Util.crc32(msg.getBody()) );			// body CRC 
				msg.setQueueId( 0 );									// queue id 后期可考虑利用
				msg.setSysFlag( 0 );
				msg.setBornTimestamp( System.currentTimeMillis() );
				
				
				PutMessageResult pmr = RedisEngineCtx.INSTANCE().getVirtualMemoryService().putMessage( msg );
				if ( pmr.isOk() ) {
					responseNode.getBufferQueue().offer( pmr );
					
				} else {
					LOGGER.warn("response append error: appendMessageResult={}, conn={}",
							new Object[] { pmr.getAppendMessageResult(), frontCon });
					
					return ResponseStatusCode.ERROR;
				}
			}
			
			responseNode.addResponseCount(count);
			
			allResponseCount.addAndGet(count);
			
			// 判断所有节点是否全部返回
			if ( allResponseCount.get()  == rrs.getTransCount() ) {
				return ResponseStatusCode.ALL_NODE_COMPLETED;
			} 
			
			// 判断当前节点是否全部返回
			if ( responseNode.getNode().getRequestIndexs().size() == responseNode.getResponseCount() ) {
				return ResponseStatusCode.THE_NODE_COMPLETED;
			}
			
			return  ResponseStatusCode.INCOMPLETE;
		}
		
        return ResponseStatusCode.ERROR;
	}
	
	
	// marge
	protected List<Object> mergeResponses() {
		
		if ( isMarged.compareAndSet(false, true) ) {
			
			Object[] responses = new Object[ rrs.getRequestCount() ];
			
			// 后端节点应答
			for(ResponseNode responseNode: responseNodeMap.values()) {
				RouteResultNode node = responseNode.getNode();
				List<Integer> idxs = node.getRequestIndexs();
				for (int index : idxs) {
					responses[index] = responseNode.bufferQueue.poll();
				}
			}
			
			// 自动应答
			if ( !rrs.getAutoResponseIndexs().isEmpty() ) {
				for (int index : rrs.getAutoResponseIndexs()) {
					responses[index] = "+OK\r\n".getBytes();
				}
			}
			return Arrays.asList(responses);
			
		} else {
			return null;
		}
	}
	
	// VM 资源清理
	private void clearVirtualMemoryResource() {
        if ( isMarged.compareAndSet(false, true) ) {
            for(ResponseNode responseNode: responseNodeMap.values()) {
                while( !responseNode.getBufferQueue().isEmpty() ) {
                    PutMessageResult pmr = responseNode.getBufferQueue().poll();
                    AppendMessageResult amr = pmr.getAppendMessageResult();
                    // 标记该消息已经被消费
                    RedisEngineCtx.INSTANCE().getVirtualMemoryService().markAsConsumed(amr.getWroteOffset(), amr.getWroteBytes());
                }
            }
        }
	}
	
	// 后端链接清理  
	// 持有连接、 处理（正确、异常）、释放连接
	// ------------------------------------------------------------
	private void clearBackendConnections() {
        for(Map.Entry<Long, RedisBackendConnection> entry: backendConnections.entrySet()) {
        	RedisBackendConnection backendConn = entry.getValue();
        	if (backendConnections.remove(entry.getKey()) != null )
        		backendConn.release();
        } 
		
        backendConnections.clear();
	}
	
	
	protected void holdBackendConnection(RedisBackendConnection backendConn) {
		backendConnections.put(backendConn.getId(), backendConn);
	}
	
	protected void releaseBackendConnection(RedisBackendConnection backendConn) {
		if (backendConnections.remove(backendConn.getId()) != null) {
			backendConn.release();
		}
	}
	// ------------------------------------------------------------
	
	
	// 消息写入出错
	protected void responseAppendError() {
		
		this.clearBackendConnections();
        
        if( frontCon != null && !frontCon.isClosed() ) {
            frontCon.writeErrMessage("response append to vm is error");
        }
        
        this.clearVirtualMemoryResource();
	}
	
    @Override
    public void backendConnectionError(Exception e) {
        super.backendConnectionError(e);
        
        clearBackendConnections();
        
        if( frontCon != null && !frontCon.isClosed() ) {
            frontCon.writeErrMessage(e.toString());
        }
        
        this.clearVirtualMemoryResource();
    }

    @Override
    public void backendConnectionClose(String reason) {
        super.backendConnectionClose(reason);
        
        clearBackendConnections();
        
        if( frontCon != null && !frontCon.isClosed() ) {
            frontCon.writeErrMessage( reason );
        }
        
        this.clearVirtualMemoryResource();
    }

}