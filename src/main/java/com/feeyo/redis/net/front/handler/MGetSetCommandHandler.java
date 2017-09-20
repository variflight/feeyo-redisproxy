package com.feeyo.redis.net.front.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.RedisEngineCtx;
import com.feeyo.redis.engine.codec.RedisPipelineResponseDecoder;
import com.feeyo.redis.engine.codec.RedisRequestType;
import com.feeyo.redis.engine.codec.RedisResponseV3;
import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.DirectTransTofrontCallBack;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.redis.virtualmemory.AppendMessageResult;
import com.feeyo.redis.virtualmemory.Message;
import com.feeyo.redis.virtualmemory.PutMessageResult;
import com.feeyo.util.ProtoUtils;

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
public class MGetSetCommandHandler extends AbstractPipelineCommandHandler {
	
    private static Logger LOGGER = LoggerFactory.getLogger(MGetSetCommandHandler.class);
    
	public final static String MSET_CMD = "MSET";
	public final static byte[] MSET_KEY = MSET_CMD.getBytes();
	public final static String MGET_CMD = "MGET";
	public final static byte[] MGET_KEY = MGET_CMD.getBytes();

    public MGetSetCommandHandler(RedisFrontConnection frontCon) {
        super(frontCon);
    }

    @Override
    protected void commonHandle(RouteResult rrs) throws IOException {
    	
    	super.commonHandle(rrs);
    	
    	// 写出
		for (RouteResultNode rrn : rrs.getRouteResultNodes()) {
			
			ByteBuffer buffer = getRequestBufferByRRN(rrn);
			
			RedisBackendConnection backendConn = null;
		    switch ( rrs.getRequestType() ) {
                case MGET:
                	backendConn = writeToBackend( rrn.getPhysicalNode(), buffer, new MGetCallBack()); 
                    break;
                case MSET:
                	backendConn = writeToBackend( rrn.getPhysicalNode(), buffer, new MSetCallBack()); 
                    break;
                default:
                    break;
		    }
		    
		    if ( backendConn != null )
				this.addBackendConnection(backendConn);
		}
		
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd( rrs.getRequestType() == RedisRequestType.MSET ? MSET_CMD : MGET_CMD );
		frontCon.getSession().setRequestKey( rrs.getRequestType() == RedisRequestType.MSET ? MSET_KEY : MGET_KEY );
		frontCon.getSession().setRequestSize( rrs.getRequestSize() );
    }
    
    private class MGetCallBack extends DirectTransTofrontCallBack {

        private RedisPipelineResponseDecoder decoder = new RedisPipelineResponseDecoder();

        @Override
        public void handleResponse(RedisBackendConnection backendCon, byte[] byteBuff) throws IOException {

            int count = decoder.parseResponseCount(byteBuff);
            if (count <= 0) {
                return;
            }

            String address = backendCon.getPhysicalNode().getName();
			byte[][] responses = decoder.getResponses();
			ResponseStatusCode state = recvResponse(address, count, responses);

			if ( state == ResponseStatusCode.ALL_NODE_COMPLETED ) {
                List<Object> resps = mergeResponses();
                if (resps != null) {
                    try {
                        String password = frontCon.getPassword();
        				String cmd = frontCon.getSession().getRequestCmd();
        				byte[] key = frontCon.getSession().getRequestKey();
                        int requestSize = frontCon.getSession().getRequestSize();
                        long requestTimeMills = frontCon.getSession().getRequestTimeMills();
                        long responseTimeMills = TimeUtil.currentTimeMillis();

                        // 长度
                        int len = 0;
                        // 数据
                        List<Message> newResponses = new ArrayList<Message>();
                        for (Object resp : resps) {
                        	if (resp instanceof PutMessageResult) {
                        		PutMessageResult pmr = (PutMessageResult) resp;
                        		AppendMessageResult amr = pmr.getAppendMessageResult();
                        		Message msg = RedisEngineCtx.INSTANCE().getVirtualMemoryService().getMessage( amr.getWroteOffset(), amr.getWroteBytes() );
                        		// 通知该消息已经被消费
                        		RedisEngineCtx.INSTANCE().getVirtualMemoryService().markAsConsumed(amr.getWroteOffset(), amr.getWroteBytes());
                        		newResponses.add(msg);
                        		len += msg.getBody().length;
                        	}
                        }

                        byte[] respCountInByte = ProtoUtils.convertIntToByteArray( rrs.getRequestCount() );
                        ByteBuffer buffer = ByteBuffer.allocate(len+1+2+respCountInByte.length);

                        buffer.put((byte)'*');
                        buffer.put(respCountInByte);
                        buffer.put("\r\n".getBytes());

                        for (Message resp : newResponses) {
                            buffer.put(resp.getBody());
                        }
                        
                        RedisResponseV3 res = new RedisResponseV3((byte)'*', buffer.array());
                        int responseSize = this.writeToFront(frontCon,res,0);

                        // 释放
                        removeAndReleaseBackendConnection(backendCon);

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
                
			} else if ( state == ResponseStatusCode.THE_NODE_COMPLETED  ) {
				
                // 如果此后端节点数据已经返回完毕，则释放链接
				removeAndReleaseBackendConnection(backendCon);
				
            } else if ( state == ResponseStatusCode.ERROR ) {
				// 添加回复到虚拟内存中出错。
				responseAppendError();
			}
        }
    }
    
    private class MSetCallBack extends DirectTransTofrontCallBack {

        private RedisPipelineResponseDecoder decoder = new RedisPipelineResponseDecoder();

        @Override
        public void handleResponse(RedisBackendConnection backendCon, byte[] byteBuff) throws IOException {

            int count = decoder.parseResponseCount(byteBuff);
            if (count <= 0) {
                return;
            }

            String address = backendCon.getPhysicalNode().getName();
            byte[][] data = decoder.getResponses();
            ResponseStatusCode state = recvResponse(address, count, data);

            if ( state == ResponseStatusCode.ALL_NODE_COMPLETED ) {
            	
                List<Object> resps = mergeResponses();
                if (resps != null) {
                    try {
                        String password = frontCon.getPassword();
                        String cmd = frontCon.getSession().getRequestCmd();
                        byte[] key = frontCon.getSession().getRequestKey();
                        int requestSize = frontCon.getSession().getRequestSize();
                        long requestTimeMills = frontCon.getSession().getRequestTimeMills();
                        long responseTimeMills = TimeUtil.currentTimeMillis();

						int responseSize = this.writeToFront(frontCon, "+OK\r\n".getBytes(), 0);

                        // 后段链接释放
                        removeAndReleaseBackendConnection(backendCon);
                        
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
                    	frontCon.releaseLock();
					}
                }
                
            } else if ( state == ResponseStatusCode.THE_NODE_COMPLETED  ) {
                // 如果此后端节点数据已经返回完毕，则释放链接
            	removeAndReleaseBackendConnection(backendCon);
            	
            } else if ( state == ResponseStatusCode.ERROR ) {
				// 添加回复到虚拟内存中出错。
				responseAppendError();
			}
        }
    }
}
