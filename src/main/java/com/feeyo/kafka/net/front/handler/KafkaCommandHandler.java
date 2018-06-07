package com.feeyo.kafka.net.front.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.feeyo.kafka.codec.Errors;
import com.feeyo.kafka.codec.FetchMetadata;
import com.feeyo.kafka.codec.FetchRequest;
import com.feeyo.kafka.codec.FetchRequest.PartitionData;
import com.feeyo.kafka.codec.FetchRequest.TopicAndPartitionData;
import com.feeyo.kafka.codec.FetchResponse;
import com.feeyo.kafka.codec.IsolationLevel;
import com.feeyo.kafka.codec.ListOffsetRequest;
import com.feeyo.kafka.codec.ListOffsetResponse;
import com.feeyo.kafka.codec.ProduceRequest;
import com.feeyo.kafka.codec.ProduceResponse;
import com.feeyo.kafka.codec.Record;
import com.feeyo.kafka.codec.RequestHeader;
import com.feeyo.kafka.net.backend.broker.BrokerApiVersion;
import com.feeyo.kafka.net.backend.broker.offset.BrokerOffsetService;
import com.feeyo.kafka.net.backend.callback.KafkaCmdCallback;
import com.feeyo.kafka.net.front.route.KafkaRouteNode;
import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Struct;
import com.feeyo.kafka.util.Utils;

import com.feeyo.redis.net.codec.RedisRequest;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.redis.net.front.handler.AbstractCommandHandler;
import com.feeyo.redis.net.front.handler.CommandParse;
import com.feeyo.redis.net.front.route.RouteResult;
import com.feeyo.redis.nio.NetSystem;
import com.feeyo.redis.nio.util.TimeUtil;
import com.feeyo.util.ProtoUtils;

public class KafkaCommandHandler extends AbstractCommandHandler {
	
	// 0表示producer无需等待leader的确认，1代表需要leader确认写入它的本地log并立即确认，-1代表所有的备份都完成后确认。
	private static final short ACKS = 1;
	private static final int PRODUCE_WAIT_TIME_MS = 500;
	private static final int CONSUME_WAIT_TIME_MS = 100;
	
	private static final int MINBYTES = 1;
	private static final int MAXBYTES = 1024 * 1024 * 4;
	
	// (isolation_level = 0) 
	private static final byte ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED.id();
	
	// Broker id of the follower. For normal consumers, use -1.
	private static final int REPLICA_ID = -1;
	private static final long LOG_START_OFFSET = -1;
	
	private static final int LENGTH_BYTE_COUNT = 4;
	private static final int PRODUCE_RESPONSE_SIZE = 2;
	private static final int CONSUMER_RESPONSE_SIZE = 3;
	private static final int OFFSET_RESPONSE_SIZE = 2;
	
	public KafkaCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}

	@Override
	protected void commonHandle(RouteResult routeResult) throws IOException {
		
		KafkaRouteNode node = (KafkaRouteNode) routeResult.getRouteNodes().get(0);
		RedisRequest request = routeResult.getRequests().get(0);
		
		ByteBuffer buffer = null;
		KafkaCmdCallback backendCallback = null;
		
		switch (request.getPolicy().getHandleType()) {
		case CommandParse.PRODUCE_CMD:
			buffer = produceEncode(request, node.getPartition());
			backendCallback = new KafkaProduceCmdCallback(node.getPartition());
			break;
			
		case CommandParse.CONSUMER_CMD:
			
			boolean isErrorOffsetRecovery = true;
			
			// 指定点位消费，消费失败不回收点位
			if (request.getNumArgs() > 2){
				isErrorOffsetRecovery = false;
			} 
			
			buffer = consumerEncode(request, node.getPartition(), node.getOffset(), node.getMaxBytes());
			backendCallback = new KafkaConsumerCmdCallback(new String(request.getArgs()[1]), node.getPartition(),
					node.getOffset(), isErrorOffsetRecovery);
			break;
			
		case CommandParse.OFFSET_CMD:
			buffer = listOffsetsEncode(request);
			backendCallback = new KafkaOffsetCmdCallback();
			break;
		}
		
		byte[] requestKey = request.getArgs()[1];
		String cmd = new String(request.getArgs()[0]).toUpperCase();
		
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd( cmd );
		frontCon.getSession().setRequestKey( new String( requestKey ) );
		frontCon.getSession().setRequestSize( buffer.position() );
		
		// 透传
		writeToBackend(node.getPhysicalNode(), buffer, backendCallback);
	}

	//
	private ByteBuffer produceEncode(RedisRequest request, int partition) {
		short version = BrokerApiVersion.getProduceVersion();
		
		Record record = new Record(0, new String(request.getArgs()[1]), request.getArgs()[1], request.getArgs()[2]);
		record.setTimestamp(TimeUtil.currentTimeMillis());
		record.setTimestampDelta(0);
		ProduceRequest pr = new ProduceRequest(version, ACKS, PRODUCE_WAIT_TIME_MS, null, partition, record);
		Struct body = pr.toStruct();
		
		RequestHeader rh = new RequestHeader(ApiKeys.PRODUCE.id, version, 
				Thread.currentThread().getName(), Utils.getCorrelationId());
		Struct header = rh.toStruct();
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( body.sizeOf() + header.sizeOf() + LENGTH_BYTE_COUNT);
		buffer.putInt(body.sizeOf() + header.sizeOf());
		header.writeTo(buffer);
		body.writeTo(buffer);
		return buffer;
	}
	
	//
	private ByteBuffer consumerEncode(RedisRequest request, int partition, long offset, int maxBytes) {
		
		short version = BrokerApiVersion.getConsumerVersion();
		
		TopicAndPartitionData<PartitionData> topicAndPartitionData = 
				new TopicAndPartitionData<PartitionData>(new String(request.getArgs()[1]));
		
		FetchRequest fr = new FetchRequest(version, REPLICA_ID, maxBytes > 10240 ? CONSUME_WAIT_TIME_MS * 5 : CONSUME_WAIT_TIME_MS, 
				MINBYTES, MAXBYTES, ISOLATION_LEVEL, 
				topicAndPartitionData, null, FetchMetadata.LEGACY);
		
		PartitionData pd = new PartitionData(offset, LOG_START_OFFSET, maxBytes);
		topicAndPartitionData.addData(partition, pd);
		
		RequestHeader rh = new RequestHeader(ApiKeys.FETCH.id, version, 
				Thread.currentThread().getName(), Utils.getCorrelationId());
		Struct header = rh.toStruct();
		Struct body = fr.toStruct();
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate(body.sizeOf() + header.sizeOf() + LENGTH_BYTE_COUNT);
		buffer.putInt(body.sizeOf() + header.sizeOf());
		header.writeTo(buffer);
		body.writeTo(buffer);
		
		return buffer;
	}
	
	//
	private ByteBuffer listOffsetsEncode(RedisRequest request) {
		
		short version = BrokerApiVersion.getListOffsetsVersion();
		
		RequestHeader rh = new RequestHeader(ApiKeys.LIST_OFFSETS.id, version, 
				Thread.currentThread().getName(), Utils.getCorrelationId());
		
		String topic = new String(request.getArgs()[1]);
		int partition = Integer.parseInt(new String(request.getArgs()[2]));
		// 根据时间查询最后此时间之后第一个点位。时间-1查询最大点位，-2查询最小点位。
		long timestamp = Long.parseLong(new String(request.getArgs()[3]));
		ListOffsetRequest lor = new ListOffsetRequest(version, topic, partition, timestamp, REPLICA_ID, ISOLATION_LEVEL);
		
		Struct header = rh.toStruct();
		Struct body = lor.toStruct();
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate(body.sizeOf() + header.sizeOf() + LENGTH_BYTE_COUNT);
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
		
		private int partition;
		
		private KafkaProduceCmdCallback(int partition) {
			this.partition = partition;
		}
		
		@Override
		public void continueParsing(ByteBuffer buffer) {
			short version = BrokerApiVersion.getProduceVersion();
			Struct response = ApiKeys.PRODUCE.parseResponse(version, buffer);
			ProduceResponse pr = new ProduceResponse(response);
			// 1k的buffer 肯定够用
			ByteBuffer bb = NetSystem.getInstance().getBufferPool().allocate(1024);
			if (pr.isCorrect()) {
				
				BrokerOffsetService.INSTANCE().updateProducerOffset(frontCon.getPassword(), pr.getTopic(), partition,
						pr.getOffset(), pr.getLogStartOffset());
				
				byte[] size = ProtoUtils.convertIntToByteArray(PRODUCE_RESPONSE_SIZE);
				byte[] partitonArr = ProtoUtils.convertIntToByteArray(partition);
				byte[] partitonLength = ProtoUtils.convertIntToByteArray(partitonArr.length);
				byte[] offsetArr = String.valueOf(pr.getOffset()).getBytes();
				byte[] offsetLength = ProtoUtils.convertIntToByteArray(offsetArr.length);
				
				bb.put(ASTERISK).put(size).put(CRLF)
					.put(DOLLAR).put(partitonLength).put(CRLF).put(partitonArr).put(CRLF)
					.put(DOLLAR).put(offsetLength).put(CRLF).put(offsetArr).put(CRLF);
				
			} else {
				byte[] size = ProtoUtils.convertIntToByteArray(1);
				byte[] msg = pr.getErrorMessage().getBytes();
				byte[] msgLen = ProtoUtils.convertIntToByteArray(msg.length);

				bb.put(ASTERISK).put(size).put(CRLF)
					.put(DOLLAR).put(msgLen).put(CRLF).put(msg).put(CRLF);
			}
			frontCon.write(bb);
		}
	}
	
	private class KafkaConsumerCmdCallback extends KafkaCmdCallback {
		
		private String topic;
		private long consumeOffset;
		private int partition;
		
		// 消费失败是否把消费点位归还（指定点位消费时，不需要归还）
		private boolean isErrorOffsetRecovery = true;
		
		private KafkaConsumerCmdCallback(String topic, int partition, long offset, boolean isErrorOffsetRecovery) {
			this.topic = topic;
			this.partition = partition;
			this.consumeOffset = offset;
			this.isErrorOffsetRecovery = isErrorOffsetRecovery;
		}
		
		@Override
		public void continueParsing(ByteBuffer buffer) {
			short version = BrokerApiVersion.getConsumerVersion();
			
			Struct response = ApiKeys.FETCH.parseResponse(version, buffer);
			FetchResponse fr = new FetchResponse(response);
			if (fr.isCorrect()) {
				List<Record> records = fr.getRecords();
				if (records == null || records.isEmpty()) {
					if ( isErrorOffsetRecovery )
						returnConsumerOffset(topic, partition, consumeOffset);
					frontCon.write(NULL);
					return;
				}
				
				byte[] size = ProtoUtils.convertIntToByteArray(CONSUMER_RESPONSE_SIZE * records.size());
				
				for (int i = 0;i<records.size();i++) {
					Record record = records.get(i);
					byte[] value = record.getValue();
					
					if (value == null) {
						if ( isErrorOffsetRecovery )
							returnConsumerOffset(topic, partition, consumeOffset);
						
						frontCon.write(NULL);
						return;
					}
					byte[] partitonArr = ProtoUtils.convertIntToByteArray(partition);
					byte[] partitonLength = ProtoUtils.convertIntToByteArray(partitonArr.length);
					byte[] offsetArr = String.valueOf(record.getOffset()).getBytes();
					byte[] offsetLength = ProtoUtils.convertIntToByteArray(offsetArr.length);
					byte[] valueLenght = ProtoUtils.convertIntToByteArray(value.length);
					
					// 计算 bufferSize $1\r\n1\r\n$4\r\n2563\r\n$4\r\ntest\r\n
					int bufferSize = 1 + size.length + 2 
							+ 1 + partitonLength.length + 2 + partitonArr.length + 2
							+ 1 + offsetLength.length + 2 + offsetArr.length + 2 
							+ 1 + valueLenght.length + 2 + value.length + 2;
					ByteBuffer bb = NetSystem.getInstance().getBufferPool().allocate(bufferSize);
					if (i == 0) {
						bb.put(ASTERISK).put(size).put(CRLF);
					}
					bb.put(DOLLAR).put(partitonLength).put(CRLF).put(partitonArr).put(CRLF)
					.put(DOLLAR).put(offsetLength).put(CRLF).put(offsetArr).put(CRLF)
					.put(DOLLAR).put(valueLenght).put(CRLF).put(value).put(CRLF);
					frontCon.write(bb);
				}
				// 消费offset超出范围
			} else if (fr.getFetchErr() != null && fr.getFetchErr().getCode() == Errors.OFFSET_OUT_OF_RANGE.code()) {
				
				if ( isErrorOffsetRecovery )
					returnConsumerOffset(topic, partition, consumeOffset);
				
				frontCon.write(NULL);
				
				// 其他错误
			} else {
				
				if ( isErrorOffsetRecovery )
					returnConsumerOffset(topic, partition, consumeOffset);
				
				StringBuffer sb = new StringBuffer();
				sb.append("-ERR ").append(fr.getErrorMessage()).append("\r\n");
				frontCon.write(sb.toString().getBytes());
			}
		}
		
		private void returnConsumerOffset(String topic, int partition, long offset) {
			BrokerOffsetService.INSTANCE().returnOffset(frontCon.getPassword(), topic, partition, offset);
		}
	}
	
	private class KafkaOffsetCmdCallback extends KafkaCmdCallback {

		@Override
		public void continueParsing(ByteBuffer buffer) {
			short version = BrokerApiVersion.getListOffsetsVersion();
			Struct response = ApiKeys.LIST_OFFSETS.parseResponse(version, buffer);
			ListOffsetResponse lor = new ListOffsetResponse(response);
			
			// 1k的buffer 肯定够用
			ByteBuffer bb = NetSystem.getInstance().getBufferPool().allocate(1024);
			if (lor.isCorrect()) {
				
				byte[] size = ProtoUtils.convertIntToByteArray(OFFSET_RESPONSE_SIZE);
				byte[] offsetArr = String.valueOf(lor.getOffset()).getBytes();
				byte[] offsetLength = ProtoUtils.convertIntToByteArray(offsetArr.length);
				byte[] timestampArr = String.valueOf(lor.getTimestamp()).getBytes();
				byte[] timestampLength = ProtoUtils.convertIntToByteArray(timestampArr.length);
				
				
				bb.put(ASTERISK).put(size).put(CRLF)
					.put(DOLLAR).put(offsetLength).put(CRLF).put(offsetArr).put(CRLF)
					.put(DOLLAR).put(timestampLength).put(CRLF).put(timestampArr).put(CRLF);
				
			} else {
				byte[] size = ProtoUtils.convertIntToByteArray(1);
				byte[] msg = lor.getErrorMessage().getBytes();
				byte[] msgLen = ProtoUtils.convertIntToByteArray(msg.length);

				bb.put(ASTERISK).put(size).put(CRLF)
					.put(DOLLAR).put(msgLen).put(CRLF).put(msg).put(CRLF);
			}
			frontCon.write(bb);
		}
		
	}
}
