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
	
	// 应答 数据索引
	public class ResponseDataIndex {
		
		public static final byte VirtualMemory = 0;		// 虚拟内存
		public static final byte HeapMemory = 1;		// 堆内存
		
		private byte type;
		
		private long offset;
		private int size;
		private byte[] data;
		
		public ResponseDataIndex(long offset, int size) {
			super();
			this.type = VirtualMemory;
			this.offset = offset;
			this.size = size;
		}

		public ResponseDataIndex( byte[] data) {
			super();
			this.type = HeapMemory;
			this.data = data;
		}
		
		public boolean isVirtualMemory() {
			return type == VirtualMemory;
		}

		public long getOffset() {
			return offset;
		}

		public int getSize() {
			return size;
		}

		public byte[] getData() {
			return data;
		}
	}

	
	// 应答节点
	public class RouteResultNodeResponse {

		private RouteResultNode node;
		
		private int count = 0;		//应答数
		private ConcurrentLinkedQueue<ResponseDataIndex> dataIndexQueue = new ConcurrentLinkedQueue<ResponseDataIndex>();
		
		public RouteResultNodeResponse(RouteResultNode node) {
			this.node = node;
		}
	}
	

	
	protected RedisRequestEncoderV2 encoder = new RedisRequestEncoderV2();
	
	protected RouteResult rrs;
	
	
	private ConcurrentHashMap<Long, RedisBackendConnection> backendConnections = new ConcurrentHashMap<Long, RedisBackendConnection>();
	
	private ConcurrentHashMap<String, RouteResultNodeResponse> responseMap =  new ConcurrentHashMap<String, RouteResultNodeResponse>(); 
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
		
		this.responseMap.clear();
		
		//
		for(RouteResultNode node: rrs.getRouteResultNodes()) {
			String address = node.getPhysicalNode().getName();
			RouteResultNodeResponse reponseNode = new RouteResultNodeResponse( node );
			responseMap.put( address, reponseNode );
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
		
		RouteResultNodeResponse nodeResponse = responseMap.get( address );
		
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
					ResponseDataIndex tmpIdx = new ResponseDataIndex( pmr.getAppendMessageResult().getWroteOffset(), pmr.getAppendMessageResult().getWroteBytes());
					nodeResponse.dataIndexQueue.offer( tmpIdx );
					
				} else {
					LOGGER.warn("response append error: appendMessageResult={}, conn={}",
							new Object[] { pmr.getAppendMessageResult(), frontCon });
					return ResponseStatusCode.ERROR;
				}
			}
			
			nodeResponse.count += count;
			
			allResponseCount.addAndGet(count);
			
			// 判断所有节点是否全部返回
			if ( allResponseCount.get()  == rrs.getTransCount() ) {
				return ResponseStatusCode.ALL_NODE_COMPLETED;
			} 
			
			// 判断当前节点是否全部返回
			if ( nodeResponse.node.getRequestIndexs().size() == nodeResponse.count ) {
				return ResponseStatusCode.THE_NODE_COMPLETED;
			}
			
			return  ResponseStatusCode.INCOMPLETE;
		}
		
        return ResponseStatusCode.ERROR;
	}
	
	
	// get & marge
	protected List<ResponseDataIndex> getAndMargeResponseDataIndexs() {
		
		if ( isMarged.compareAndSet(false, true) ) {
			
			ResponseDataIndex[] responseDataIndexs = new ResponseDataIndex[ rrs.getRequestCount() ];
			
			// 后端节点应答
			for(RouteResultNodeResponse responseNode: responseMap.values()) {
				List<Integer> idxs = responseNode.node.getRequestIndexs();
				for (int index : idxs) {
					responseDataIndexs[index] = responseNode.dataIndexQueue.poll();
				}
			}
			
			// 自动应答
			if ( !rrs.getAutoResponseIndexs().isEmpty() ) {
				for (int index : rrs.getAutoResponseIndexs()) {
					responseDataIndexs[index] = new ResponseDataIndex( "+OK\r\n".getBytes() );
				}
			}
			return Arrays.asList(responseDataIndexs);
			
		} else {
			return null;
		}
	}
	
	// VM 资源清理
	private void clearVirtualMemoryResource() {
        if ( isMarged.compareAndSet(false, true) ) {
            for(RouteResultNodeResponse responseNode: responseMap.values()) {
                while( !responseNode.dataIndexQueue.isEmpty() ) {
                    // 标记该消息已经被消费
                    ResponseDataIndex dataIdx = responseNode.dataIndexQueue.poll();
                    RedisEngineCtx.INSTANCE().getVirtualMemoryService().markAsConsumed(dataIdx.getOffset(), dataIdx.getSize());
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