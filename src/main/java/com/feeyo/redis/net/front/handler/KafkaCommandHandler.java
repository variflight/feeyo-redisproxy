package com.feeyo.redis.net.front.handler;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.feeyo.redis.config.kafka.MetaDataOffset;
import com.feeyo.redis.kafka.codec.Errors;
import com.feeyo.redis.kafka.codec.FetchRequest;
import com.feeyo.redis.kafka.codec.FetchRequest.PartitionData;
import com.feeyo.redis.kafka.codec.FetchRequest.TopicAndPartitionData;
import com.feeyo.redis.kafka.codec.FetchResponse;
import com.feeyo.redis.kafka.codec.ProduceRequest;
import com.feeyo.redis.kafka.codec.ProduceResponse;
import com.feeyo.redis.kafka.codec.Record;
import com.feeyo.redis.kafka.codec.RequestHeader;
import com.feeyo.redis.kafka.protocol.ApiKeys;
import com.feeyo.redis.kafka.protocol.types.Struct;
import com.feeyo.redis.kafka.util.Utils;
import com.feeyo.redis.net.backend.callback.KafkaCmdCallback;
import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.codec.RedisRequestPolicy;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.net.front.route.RouteResultNode;
import com.feeyo.redis.nio.NetSystem;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.ProtoUtils;

public class KafkaCommandHandler extends AbstractCommandHandler {
	private MetaDataOffset metaDataOffset;
	private long offset;
	
	public KafkaCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}

	@Override
	protected void commonHandle(RouteResult routeResult) throws IOException {
		
		RouteResultNode node = routeResult.getRouteResultNodes().get(0);
		RedisRequest request = routeResult.getRequests().get(0);
		
		ByteBuffer buffer;
		RedisRequestPolicy policy = request.getPolicy();
		KafkaCmdCallback callBack;
		this.metaDataOffset = node.getKafkaMetaDataOffset();
		if (policy.getHandleType() == CommandParse.PRODUCE_CMD) {
			buffer = produceEncode(request, metaDataOffset.getPartition());
			callBack = new KafkaProduceCmdCallback();
		} else {
			String consumer = frontCon.getPassword();
			this.offset = metaDataOffset.getConsumerOffset(consumer);
			buffer = consumerEncode(request, metaDataOffset.getPartition(), this.offset);
			callBack = new KafkaConsumerCmdCallback();
		} 
		
		byte[] requestKey = request.getArgs()[1];
		String cmd = new String(request.getArgs()[0]).toUpperCase();
		
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd( cmd );
		frontCon.getSession().setRequestKey( requestKey );
		frontCon.getSession().setRequestSize( buffer.position() );
		
		// 透传
		writeToBackend(node.getPhysicalNode(), buffer, callBack);
	}

	private ByteBuffer produceEncode(RedisRequest request, int partition) {
		Record record = new Record();
		record.setOffset(0);
		record.setKey(request.getArgs()[1]);
		record.setValue(request.getArgs()[2]);
		record.setTopic(new String(request.getArgs()[1]));
		record.setTimestamp(TimeUtil.currentTimeMillis());
		record.setTimestampDelta(0);
		ProduceRequest pr = new ProduceRequest((short)5, (short)1, 30000, null, partition, record);
		Struct body = pr.toStruct();
		RequestHeader rh = new RequestHeader(ApiKeys.PRODUCE.id, (short)5, Thread.currentThread().getName(), Utils.getCorrelationId());
		Struct header = rh.toStruct();
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( body.sizeOf() + header.sizeOf() + 4);
		buffer.putInt(body.sizeOf() + header.sizeOf());
		header.writeTo(buffer);
		body.writeTo(buffer);
		return buffer;
	}
	
	private ByteBuffer consumerEncode(RedisRequest request, int partition, long offset) {
		TopicAndPartitionData<PartitionData> topicAndPartitionData = new TopicAndPartitionData<PartitionData>(new String(request.getArgs()[1]));
//		public FetchRequest(short version, int replicaId, int maxWait, int minBytes, int maxBytes,
//                byte isolationLevel, TopicAndPartitionData<PartitionData> topicAndPartitionData)
		FetchRequest fr = new FetchRequest((short)6, -1, 500, 1, 1024*1024, (byte)0, topicAndPartitionData);
		PartitionData pd = new PartitionData(offset, -1, 1);
		topicAndPartitionData.addData(partition, pd);
		RequestHeader rh = new RequestHeader(ApiKeys.FETCH.id, (short)6, Thread.currentThread().getName(), 1);
		Struct header = rh.toStruct();
		Struct body = fr.toStruct();
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate(body.sizeOf() + header.sizeOf() + 4);
		buffer.putInt(body.sizeOf() + header.sizeOf());
		header.writeTo(buffer);
		body.writeTo(buffer);
		
		return buffer;
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
	
	private class KafkaProduceCmdCallback extends KafkaCmdCallback {
		
		@Override
		public void handle(ByteBuffer buffer) {
			Struct response = ApiKeys.PRODUCE.parseResponse((short)5, buffer);
			ProduceResponse pr = new ProduceResponse(response);
			if (pr.isCorrect()) {
				frontCon.write(OK);
			} else {
				StringBuffer sb = new StringBuffer();
				sb.append("-ERR ").append(pr.getErrorMessage()).append("\r\n");
				frontCon.write(sb.toString().getBytes());
			}
		}
	}
	
	private class KafkaConsumerCmdCallback extends KafkaCmdCallback {
		
		@Override
		public void handle(ByteBuffer buffer) {
			Struct response = ApiKeys.FETCH.parseResponse((short)6, buffer);
			FetchResponse fr = new FetchResponse(response);
			if (fr.isCorrect()) {
				byte[] value = fr.getRecord().getValue();
				if (value == null) {
					metaDataOffset.sendDefaultOffsetBack(offset, frontCon.getPassword());
					frontCon.write(NULL);
					return;
				}
				byte[] size = ProtoUtils.convertIntToByteArray(1);
				byte[] valueLenght = ProtoUtils.convertIntToByteArray(value.length);
					
				// 计算 bufferSize *1\r\n$4\r\ntest\r\n
				int bufferSize = 1 + size.length + 2 + 1 + valueLenght.length + 2 + value.length + 2;
				ByteBuffer bb = NetSystem.getInstance().getBufferPool().allocate(bufferSize);
				bb.put(ASTERISK).put(size).put(CRLF).put(DOLLAR).put(valueLenght).put(CRLF).put(value).put(CRLF);
				
				frontCon.write(bb);
				
			// 消费offset超出范围
			} else if (fr.getFetchErr() != null && fr.getFetchErr().getCode() == Errors.OFFSET_OUT_OF_RANGE.code()) {
				metaDataOffset.sendDefaultOffsetBack(offset, frontCon.getPassword());
				frontCon.write(NULL);
				
			// 其他错误
			} else {
				metaDataOffset.sendDefaultOffsetBack(offset, frontCon.getPassword());
				StringBuffer sb = new StringBuffer();
				sb.append("-ERR ").append(fr.getErrorMessage()).append("\r\n");
				frontCon.write(sb.toString().getBytes());
			}
		}
	}
	
}
