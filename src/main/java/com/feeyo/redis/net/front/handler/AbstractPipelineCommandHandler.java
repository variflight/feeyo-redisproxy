package com.feeyo.redis.net.front.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.net.codec.redis.RedisRequestEncoder;
import com.feeyo.net.codec.redis.RedisResponse;
import com.feeyo.net.codec.redis.RedisResponseDecoder;
import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.handler.segment.Segment;
import com.feeyo.redis.net.front.handler.segment.SegmentType;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteNode;
import com.feeyo.redis.virtualmemory.Message;
import com.feeyo.redis.virtualmemory.PutMessageResult;
import com.feeyo.redis.virtualmemory.Util;
import com.feeyo.util.ProtoUtils;

/**
 * 抽象 pipeline 处理
 *
 */
public abstract class AbstractPipelineCommandHandler extends AbstractCommandHandler {
	
	private static Logger LOGGER = LoggerFactory.getLogger( AbstractPipelineCommandHandler.class );
	
	// VM数据偏移
	public class DataOffset {
		
		public static final byte VIRTUAL_MEMORY = 0;	// 虚拟内存
		public static final byte HEAP_MEMORY = 1;		// 堆内存
		
		private byte type;
		
		private long offset;
		private int size;
		private byte[] data;
		
		public DataOffset(long offset, int size) {
			super();
			this.type = VIRTUAL_MEMORY;
			this.offset = offset;
			this.size = size;
		}

		public DataOffset( byte[] data) {
			super();
			this.type = HEAP_MEMORY;
			this.data = data;
		}

		public long getOffset() {
			return offset;
		}

		public int getSize() {
			return size;
		}

		public byte[] getData() {
			if ( type == VIRTUAL_MEMORY ) {
				return RedisEngineCtx.INSTANCE().getVirtualMemoryService().getMessageBodyAndMarkAsConsumed( offset, size );
			}
			return data;
		}
		
		public void clearData() {
			if ( type == VIRTUAL_MEMORY ) {
				RedisEngineCtx.INSTANCE().getVirtualMemoryService().markAsConsumed( offset, size );
			}
		}
	}
	
	// 应答合并结果
	public class ResponseMargeResult {

		public static final byte ERROR =  0;					//错误
		public static final byte INCOMPLETE = 1;				//未完整
		public static final byte THE_NODE_COMPLETED = 2;		//当前节点完成
		public static final byte ALL_NODE_COMPLETED = 9; 		//所有节点都完成
		
		
		private byte status;
		private List<DataOffset> offsets = null;
		
		public ResponseMargeResult(byte status) {
			super();
			this.status = status;
		}

		public ResponseMargeResult(byte status, List<DataOffset> offsets) {
			super();
			this.status = status;
			this.offsets = offsets;
		}

		public byte getStatus() {
			return status;
		}

		public List<DataOffset> getDataOffsets() {
			return offsets;
		}
		
	}
	
	
	// 应答节点
	public class ResponseNode {

		private RouteNode sourceNode;
		
		private int count = 0;		//应答数
		private ConcurrentLinkedQueue<DataOffset> dataOffsetQueue = new ConcurrentLinkedQueue<DataOffset>();
		
		public ResponseNode(RouteNode node) {
			this.sourceNode = node;
		}
	}

	protected RedisRequestEncoder encoder = new RedisRequestEncoder();
	protected RouteResult rrs;
	
	private ConcurrentHashMap<String, ResponseNode> allResponseNode =  new ConcurrentHashMap<String, ResponseNode>(); 
	private AtomicInteger allResponseCount = new AtomicInteger(0); 					// 接收到返回数据的条数
	
	private ConcurrentHashMap<Long, BackendConnection> backendConnections = new ConcurrentHashMap<Long, BackendConnection>();
	


	public AbstractPipelineCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}
	
	@Override
	protected void commonHandle(RouteResult rrs) throws IOException {
		
		this.rrs = rrs;
		this.allResponseCount.set(0);
		this.allResponseNode.clear();
		
		//
		for(RouteNode node: rrs.getRouteNodes()) {
			String address = node.getPhysicalNode().getName();
			ResponseNode reponseNode = new ResponseNode( node );
			allResponseNode.put( address, reponseNode );
		}
	}
	
	
	protected ByteBuffer getRequestBufferByRRN(RouteNode rrn) {
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
	protected synchronized ResponseMargeResult addAndMargeResponse(String address, int count, byte[][] resps) {
		
		ResponseNode node = allResponseNode.get( address );
		
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
					DataOffset dataOffset = new DataOffset( pmr.getAppendMessageResult().getWroteOffset(), pmr.getAppendMessageResult().getWroteBytes());
					node.dataOffsetQueue.offer( dataOffset );
					
				} else {
					LOGGER.warn("response append error: appendMessageResult={}, conn={}",
							new Object[] { pmr.getAppendMessageResult(), frontCon });
					return new ResponseMargeResult( ResponseMargeResult.ERROR );
				}
			}
			
			node.count += count;
			
			allResponseCount.addAndGet(count);
			
			// 判断所有节点是否全部返回
			if ( allResponseCount.get()  == rrs.getThroughtCount() ) {
				
				//合并结果
				DataOffset[] offsets = new DataOffset[ rrs.getRequestCount() ];
				for(ResponseNode responseNode: allResponseNode.values()) {							//
					List<Integer> idxs = responseNode.sourceNode.getRequestIndexs();
					for (int index : idxs) {
						offsets[index] = responseNode.dataOffsetQueue.poll();
					}
				}
				
				if ( rrs.getNoThroughtIndexs() != null && !rrs.getNoThroughtIndexs().isEmpty() ) {
					for (int index : rrs.getNoThroughtIndexs()) {
						offsets[index] = new DataOffset( "+OK\r\n".getBytes() );
					}
				}
				
				
				// segments pack
				if(null != rrs.getSegments() && !rrs.getSegments().isEmpty()) {
					
					List<DataOffset> newDataOffsets = new ArrayList<DataOffset>();
				
					List<Segment> segments = rrs.getSegments();
					for (Segment seg : segments) {
						int[] indexs = seg.getIndexs();
						SegmentType type = seg.getType();
						
						//组包
						List<DataOffset> segmentDataOffset = pack(Arrays.asList(offsets), type, indexs);		
						newDataOffsets.addAll( segmentDataOffset );
					}
					
					return new ResponseMargeResult( ResponseMargeResult.ALL_NODE_COMPLETED, newDataOffsets);
				}
				return new ResponseMargeResult( ResponseMargeResult.ALL_NODE_COMPLETED, Arrays.asList(offsets));
			} 
			
			// 判断当前节点是否全部返回
			if ( node.sourceNode.getRequestIndexs().size() == node.count ) {
				return new ResponseMargeResult( ResponseMargeResult.THE_NODE_COMPLETED );
			}
			
			return new ResponseMargeResult( ResponseMargeResult.INCOMPLETE );
		}
		
        return new ResponseMargeResult( ResponseMargeResult.ERROR );
	}
	
	private List<DataOffset> pack(List<DataOffset> offsets, SegmentType type, int[] indexs) {
		
		List<DataOffset> newDataOffsets = new ArrayList<DataOffset>();
		
		switch (type) {
		case MGET:
			int len = 0;
			List<byte[]> newResponses = new ArrayList<byte[]>();
			for (int index : indexs) {
				DataOffset offset = offsets.get(index);
				byte[] data = offset.getData();
				newResponses.add(data);
				len += data.length;
			}
			byte[] respCountInByte = ProtoUtils.convertIntToByteArray(indexs.length);
			ByteBuffer buffer = ByteBuffer.allocate(len + 1 + 2 + respCountInByte.length);

			buffer.put((byte) '*');
			buffer.put(respCountInByte);
			buffer.put("\r\n".getBytes());

			for (byte[] respData : newResponses) {
				buffer.put(respData);
			}
			newDataOffsets.add(new DataOffset(buffer.array()));
			break;
		case MSET:
			newDataOffsets.add(new DataOffset("+OK\r\n".getBytes()));
			for (int i : indexs) {
				offsets.get(i).clearData();
			}
			break;
		case MDEL:
		case MEXISTS:
			RedisResponseDecoder responseDecoder = new RedisResponseDecoder();
			int okCount = 0;
			for (DataOffset offset : offsets) {
				byte[] data = offset.getData();
				RedisResponse response = responseDecoder.decode( data ).get(0);
				if ( response.is( (byte)':') ) {
					byte[] _buf1 = (byte[])response.data();
					byte[] buf2 = new byte[ _buf1.length - 1 ];
					System.arraycopy(_buf1, 1, buf2, 0, buf2.length);
					int c = readInt( buf2 );
					okCount += c;
				}
			}
			newDataOffsets.add(new DataOffset(encode(okCount)));
			for( int i :indexs) {
				offsets.get(i).clearData();
			}
			break;
		case DEFAULT:
			newDataOffsets.add(offsets.get(indexs[0]));
			break;
		}
		
		return newDataOffsets;
	}
	
	private int readInt(byte[] buf) {
		int idx = 0;
		final boolean isNeg = buf[idx] == '-';
		if (isNeg) {
			++idx;
		}
		int value = 0;
		while (true) {
			final int b = buf[idx++];
			if (b == '\r' && buf[idx++] == '\n') {
				break;
			} else {
				value = value * 10 + b - '0';
			}
		}
		return value;
	}
	
	private byte[] encode(int count) {
		// 编码
		byte[] countBytes = ProtoUtils.convertIntToByteArray( count );
        ByteBuffer buffer = ByteBuffer.allocate( 1 + 2 + countBytes.length); //   cmd length + \r\n length + count bytes length
        buffer.put((byte)':');
        buffer.put( countBytes );
        buffer.put("\r\n".getBytes());
        
        buffer.flip();
        byte[] data = new byte[ buffer.remaining() ];
        buffer.get( data );
        return data;
	}
	
	
	// VM 资源清理
	private synchronized void clearVirtualMemoryResource() {
		for(ResponseNode node: allResponseNode.values()) {
            while( !node.dataOffsetQueue.isEmpty() ) {
                // 标记该消息已经被消费
                DataOffset dataOffset = node.dataOffsetQueue.poll();
                if ( dataOffset != null ) {
                	dataOffset.clearData();
                }
            }
        }
	}
	
	// 后端链接清理  
	// 持有连接、 处理（正确、异常）、释放连接
	// ------------------------------------------------------------
	private void clearBackendConnections() {
        for(Map.Entry<Long, BackendConnection> entry: backendConnections.entrySet()) {
        	BackendConnection backendConn = entry.getValue();
        	if (backendConnections.remove(entry.getKey()) != null )
        		backendConn.release();
        } 
		
        backendConnections.clear();
	}
	
	
	protected void holdBackendConnection(BackendConnection backendConn) {
		backendConnections.put(backendConn.getId(), backendConn);
	}
	
	protected void releaseBackendConnection(BackendConnection backendConn) {
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