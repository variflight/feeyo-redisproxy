package com.feeyo.kafka.net.front.handler;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.feeyo.kafka.net.backend.callback.KafkaCmdCallback;
import com.feeyo.kafka.net.backend.callback.KafkaConsumerCmdCallback;
import com.feeyo.kafka.net.backend.callback.KafkaOffsetCmdCallback;
import com.feeyo.kafka.net.backend.callback.KafkaProduceCmdCallback;
import com.feeyo.kafka.net.front.route.KafkaRouteNode;
import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.handler.AbstractCommandHandler;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.route.RouteResult;

public class KafkaCommandHandler extends AbstractCommandHandler {
	//
	// encoder
	private KafkaEncoder encoder = new KafkaEncoder();
	
	public KafkaCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}

	@Override
	protected void commonHandle(RouteResult routeResult) throws IOException {
		
		KafkaRouteNode node = (KafkaRouteNode) routeResult.getRouteNodes().get(0);
		//
		ByteBuffer requestBuf = null;
		KafkaCmdCallback callback = null;
		
		RedisRequest request = routeResult.getRequests().get(0);
		switch (request.getPolicy().getHandleType()) {
			case CommandParse.PRODUCE_CMD: {
				int partition = node.getPartition();
				requestBuf = encoder.encodeProduceRequest(request, partition);
				callback = new KafkaProduceCmdCallback(node.getPartition());
				break;
			}
			case CommandParse.CONSUMER_CMD: {
				// 指定点位消费，消费失败不回收点位
				boolean isErrorOffsetRecovery = request.getNumArgs() > 2 ? false : true;
				String topic = new String(request.getArgs()[1]);
				int partition = node.getPartition();
				long offset = node.getOffset();
				int maxBytes = node.getMaxBytes();
	
				requestBuf = encoder.encodeFetchRequest(request, partition, offset, maxBytes);
				callback = new KafkaConsumerCmdCallback(topic, partition, offset, isErrorOffsetRecovery);
				break;
			}
			case CommandParse.OFFSET_CMD: {
				requestBuf = encoder.encodeListOffsetRequest(request);
				callback = new KafkaOffsetCmdCallback();
				break;
			}
		}
		//
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd(new String(request.getArgs()[0]).toUpperCase());
		frontCon.getSession().setRequestKey(new String(request.getArgs()[1]));
		if (requestBuf != null) {
			frontCon.getSession().setRequestSize(requestBuf.position());
		}
		//
		// 透传
		this.writeToBackend(node.getPhysicalNode(), requestBuf, callback);
	}

	@Override
	public void frontConnectionClose(String reason) {
		super.frontConnectionClose(reason);
	}
	
	@Override
    public void backendConnectionError(Exception e) {
		super.backendConnectionError(e);
		if( frontCon != null && !frontCon.isClosed() ) {
			frontCon.writeErrMessage(e.toString());
		}
	}

	@Override
	public void backendConnectionClose(String reason) {
		super.backendConnectionClose(reason);
		if( frontCon != null && !frontCon.isClosed() ) {
			frontCon.writeErrMessage( reason );
		}
	}
}