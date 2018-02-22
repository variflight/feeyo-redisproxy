package com.feeyo.redis.net.front.handler.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.codec.RedisPipelineResponseDecoderV2;
import com.feeyo.redis.engine.codec.RedisPipelineResponseDecoderV2.PipelineResponse;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.DirectTransTofrontCallBack;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.handler.AbstractPipelineCommandHandler;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.nio.util.TimeUtil;

/**
 * handler for mget and mset command in clust pool
 *
 * <p> for standalone redis-server we have: <pre>
 *  127.0.0.1:6379> mset a1 aa b1 bb
 *  OK
 *  127.0.0.1:6379> mget a1 b1
 *  1) "aa"
 *  2) "bb"
 *  127.0.0.1:6379>
 *   
 * @author Tr!bf wangyamin@variflight.com
 */
public class SegmentCommandHandler extends AbstractPipelineCommandHandler {
	
    private static Logger LOGGER = LoggerFactory.getLogger(SegmentCommandHandler.class);
    
    public SegmentCommandHandler(RedisFrontConnection frontCon) {
        super(frontCon);
    }

    @Override
    protected void commonHandle(RouteResult rrs) throws IOException {
    	
    	super.commonHandle(rrs);
    	
    	// 写出
		for (RouteResultNode rrn : rrs.getRouteResultNodes()) {
			
			ByteBuffer buffer = getRequestBufferByRRN(rrn);
			
		    RedisBackendConnection backendConn = writeToBackend( rrn.getPhysicalNode(), buffer, new SegmentCallBack()); 
		    
		    if ( backendConn != null )
				this.holdBackendConnection(backendConn);
		}
		
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd( rrs.getRequestType().getCmd());
		frontCon.getSession().setRequestKey( rrs.getRequestType().getCmd().getBytes());
		frontCon.getSession().setRequestSize( rrs.getRequestSize() );
    }
    
    private class SegmentCallBack extends DirectTransTofrontCallBack {

        private RedisPipelineResponseDecoderV2 decoder = new RedisPipelineResponseDecoderV2();

        @Override
        public void handleResponse(RedisBackendConnection backendCon, ByteBuffer byteBuff) throws IOException {

        	PipelineResponse pipelineResponse = decoder.parse(byteBuff);
        	if ( !pipelineResponse.isOK() )
                return;

            String address = backendCon.getPhysicalNode().getName();
            
            ResponseMargeResult result = addAndMargeResponse(address, pipelineResponse.getCount(), pipelineResponse.getResps());
			if ( result.getStatus() == ResponseMargeResult.ALL_NODE_COMPLETED ) {
				List<DataOffset> offsets = result.getDataOffsets();
				if (offsets != null) {
                    try {
                        String password = frontCon.getPassword();
        				String cmd = frontCon.getSession().getRequestCmd();
        				byte[] key = frontCon.getSession().getRequestKey();
                        int requestSize = frontCon.getSession().getRequestSize();
                        long requestTimeMills = frontCon.getSession().getRequestTimeMills();
                        long responseTimeMills = TimeUtil.currentTimeMillis();

                        int responseSize = 0;
                        for (DataOffset offset : offsets) {
							byte[] data = offset.getData();
							responseSize += this.writeToFront(frontCon, data, 0);
						}
                        
                        // 释放
                        releaseBackendConnection(backendCon);

                        // 数据收集
                        StatUtil.collect(password, cmd, key, requestSize, responseSize,
                                (int) (responseTimeMills - requestTimeMills), false);
                        
                    } catch (IOException e2) {
                        if (frontCon != null) {
                            frontCon.close("write err");
                        }

                        // 由 reactor close
                        LOGGER.error("backend write to front err:", e2);
                        throw e2;
                        
                    } finally {
						// 释放锁
                    	frontCon.releaseLock();;
					}
                }
                
			} else if ( result.getStatus() == ResponseMargeResult.THE_NODE_COMPLETED  ) {
				
                // 如果此后端节点数据已经返回完毕，则释放链接
				releaseBackendConnection(backendCon);
				
            } else if ( result.getStatus() == ResponseMargeResult.ERROR ) {
				// 添加回复到虚拟内存中出错。
				responseAppendError();
			}
        }
    }
}
