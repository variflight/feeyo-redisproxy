package com.feeyo.redis.net.front.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.manage.stat.StatUtil;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.backend.RedisBackendConnection;
import com.feeyo.redis.net.backend.callback.DirectTransTofrontCallBack;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisRequestType;
import com.feeyo.redis.net.codec.RedisResponsePipelineDecoder;
import com.feeyo.redis.net.codec.RedisResponsePipelineDecoder.PipelineResponse;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.nio.util.TimeUtil;

public class PipelineCommandHandler extends AbstractPipelineCommandHandler {
	
	private static Logger LOGGER = LoggerFactory.getLogger(PipelineCommandHandler.class);
	
	public PipelineCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}
	
	@Override
	protected void commonHandle(RouteResult rrs) throws IOException {
		
		super.commonHandle(rrs);
		
    	// 写出
		for (RouteResultNode rrn : rrs.getRouteResultNodes()) {
			ByteBuffer buffer =  getRequestBufferByRRN(rrn);
			RedisBackendConnection backendConn = writeToBackend(rrn.getPhysicalNode(), buffer, new PipelineDirectTransTofrontCallBack());
			if ( backendConn != null )
				this.holdBackendConnection( backendConn );
		}
		
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd( RedisRequestType.PIPELINE.getCmd() );
		frontCon.getSession().setRequestKey( RedisRequestType.PIPELINE.getCmd().getBytes() );
		frontCon.getSession().setRequestSize( rrs.getRequestSize() );
	}
	
	// 
	private class PipelineDirectTransTofrontCallBack extends DirectTransTofrontCallBack {

		private RedisResponsePipelineDecoder decoder = new RedisResponsePipelineDecoder();
		
		@Override
		public void handleResponse(BackendConnection backendCon, byte[] byteBuff) throws IOException {

			// 解析此次返回的数据条数
			PipelineResponse pipelineResponse = decoder.parse( byteBuff );
			if ( !pipelineResponse.isOK() )
				return;
			
			// 这里缓存进文件的是 pipelienDecoder中缓存的数据。 防止断包之后丢数据
			String address = backendCon.getPhysicalNode().getName();
			ResponseMargeResult result = addAndMargeResponse(address, pipelineResponse.getCount(), pipelineResponse.getResps());
			
			// 如果所有请求，应答都已经返回
			if ( result.getStatus() == ResponseMargeResult.ALL_NODE_COMPLETED ) {
				
				// 获取所有应答
				List<DataOffset> offsets = result.getDataOffsets();
				if (offsets != null) {

					try {
						String password = frontCon.getPassword();
						int requestSize = frontCon.getSession().getRequestSize();
						long requestTimeMills = frontCon.getSession().getRequestTimeMills();
						long responseTimeMills = TimeUtil.currentTimeMillis();
						int responseSize = 0;

						for (DataOffset offset : offsets) {
							byte[] data = offset.getData();
							responseSize += this.writeToFront(frontCon, data, 0);
						}

						// 后段链接释放
						releaseBackendConnection(backendCon);

						// 数据收集
						StatUtil.collect(password, RedisRequestType.PIPELINE.getCmd(), RedisRequestType.PIPELINE.getCmd().getBytes(), requestSize, responseSize,
								(int) (responseTimeMills - requestTimeMills), false);

						// child 收集
						for (RedisRequest req : rrs.getRequests()) {
							String childCmd = new String( req.getArgs()[0] );
							byte[] requestKey = req.getNumArgs() > 1 ? req.getArgs()[1] : null;
							StatUtil.collect(password, childCmd, requestKey, requestSize, responseSize,
									(int) (responseTimeMills - requestTimeMills), true);
						}
						
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
