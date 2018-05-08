package com.feeyo.kafka.net.front.handler;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.feeyo.kafka.codec.Errors;
import com.feeyo.kafka.codec.FetchRequest;
import com.feeyo.kafka.codec.FetchRequest.PartitionData;
import com.feeyo.kafka.codec.FetchRequest.TopicAndPartitionData;
import com.feeyo.kafka.codec.FetchResponse;
import com.feeyo.kafka.codec.ProduceRequest;
import com.feeyo.kafka.codec.ProduceResponse;
import com.feeyo.kafka.codec.Record;
import com.feeyo.kafka.codec.RequestHeader;
import com.feeyo.kafka.config.Metadata;
import com.feeyo.kafka.config.ConsumerOffset;
import com.feeyo.kafka.config.DataOffset;
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
	private static final int TIME_OUT = 1000;
	private static final int MINBYTES = 1;
	private static final int MAXBYTES = 1024 * 1024 * 4;
	
	// (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1)
	private static final byte ISOLATION_LEVEL = 0;
	
	// Broker id of the follower. For normal consumers, use -1.
	private static final int REPLICA_ID = -1;
	private static final long LOG_START_OFFSET = -1;
	
	// 一次消费的条数
	private static final int FETCH_LOG_COUNT = 1;
	private static final int LENGTH_BYTE_COUNT = 4;
	
	
	public KafkaCommandHandler(RedisFrontConnection frontCon) {
		super(frontCon);
	}

	@Override
	protected void commonHandle(RouteResult routeResult) throws IOException {
		
		KafkaRouteNode node = (KafkaRouteNode) routeResult.getRouteNodes().get(0);
		RedisRequest request = routeResult.getRequests().get(0);
		
		ByteBuffer buffer;
		KafkaCmdCallback backendCallback;
		
		if ( request.getPolicy().getHandleType() == CommandParse.PRODUCE_CMD ) {
			
			buffer = produceEncode(request, node.getDataOffset().getPartition());
			backendCallback = new KafkaProduceCmdCallback( node.getDataOffset() );
			
		} else {
			
			// 指定点位消费
			long newOffset;
			boolean isErrorOffsetRecovery = true;
			if (request.getNumArgs() == 4) {
				newOffset = Long.parseLong(new String(request.getArgs()[3]));
				isErrorOffsetRecovery = false;
			} else {
				
				ConsumerOffset cOffset =  node.getDataOffset().getConsumerOffsetByConsumer( frontCon.getPassword() );
				newOffset =	cOffset.getNewOffset();
			}
			
			
			buffer = consumerEncode(request, node.getDataOffset().getPartition(), newOffset);
			backendCallback = new KafkaConsumerCmdCallback( node.getDataOffset(), newOffset, isErrorOffsetRecovery );
		} 
		
		byte[] requestKey = request.getArgs()[1];
		String cmd = new String(request.getArgs()[0]).toUpperCase();
		
		// 埋点
		frontCon.getSession().setRequestTimeMills(TimeUtil.currentTimeMillis());
		frontCon.getSession().setRequestCmd( cmd );
		frontCon.getSession().setRequestKey( requestKey );
		frontCon.getSession().setRequestSize( buffer.position() );
		
		// 透传
		writeToBackend(node.getPhysicalNode(), buffer, backendCallback);
	}

	private ByteBuffer produceEncode(RedisRequest request, int partition) {
		short version = Metadata.getProduceVersion();
		
		Record record = new Record();
		record.setOffset(0);
		record.setKey(request.getArgs()[1]);
		record.setValue(request.getArgs()[2]);
		record.setTopic(new String(request.getArgs()[1]));
		record.setTimestamp(TimeUtil.currentTimeMillis());
		record.setTimestampDelta(0);
		ProduceRequest pr = new ProduceRequest(version, ACKS, TIME_OUT, null, partition, record);
		Struct body = pr.toStruct();
		RequestHeader rh = new RequestHeader(ApiKeys.PRODUCE.id, version, Thread.currentThread().getName(), Utils.getCorrelationId());
		Struct header = rh.toStruct();
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( body.sizeOf() + header.sizeOf() + LENGTH_BYTE_COUNT);
		buffer.putInt(body.sizeOf() + header.sizeOf());
		header.writeTo(buffer);
		body.writeTo(buffer);
		return buffer;
	}
	
	private ByteBuffer consumerEncode(RedisRequest request, int partition, long offset) {
		short version = Metadata.getConsumerVersion();
		
		TopicAndPartitionData<PartitionData> topicAndPartitionData = new TopicAndPartitionData<PartitionData>(new String(request.getArgs()[1]));
		FetchRequest fr = new FetchRequest(version, REPLICA_ID, TIME_OUT, MINBYTES, MAXBYTES, ISOLATION_LEVEL, topicAndPartitionData);
		PartitionData pd = new PartitionData(offset, LOG_START_OFFSET, FETCH_LOG_COUNT);
		topicAndPartitionData.addData(partition, pd);
		RequestHeader rh = new RequestHeader(ApiKeys.FETCH.id, version, Thread.currentThread().getName(), Utils.getCorrelationId());
		Struct header = rh.toStruct();
		Struct body = fr.toStruct();
		
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
		
		private DataOffset dataOffset;
		
		private KafkaProduceCmdCallback(DataOffset dataOffset) {
			this.dataOffset = dataOffset;
		}
		
		@Override
		public void continueParsing(ByteBuffer buffer) {
			Struct response = ApiKeys.PRODUCE.parseResponse((short)5, buffer);
			ProduceResponse pr = new ProduceResponse(response);
			if (pr.isCorrect()) {
				frontCon.write(OK);
				dataOffset.setProducerOffset(pr.getOffset(), pr.getLogStartOffset());
			} else {
				StringBuffer sb = new StringBuffer();
				sb.append("-ERR ").append(pr.getErrorMessage()).append("\r\n");
				frontCon.write(sb.toString().getBytes());
			}
		}
	}
	
	private class KafkaConsumerCmdCallback extends KafkaCmdCallback {
		
		private DataOffset dataOffset;
		private long consumeOffset;
		
		// 消费失败是否把消费点位归还（指定点位消费时，不需要归还）
		private boolean isErrorOffsetRecovery = true;
		
		private KafkaConsumerCmdCallback(DataOffset dataOffset, long newOffset, boolean isErrorOffsetRecovery) {
			this.dataOffset = dataOffset;
			this.consumeOffset = newOffset;
			this.isErrorOffsetRecovery = isErrorOffsetRecovery;
		}
		
		@Override
		public void continueParsing(ByteBuffer buffer) {
			

			Struct response = ApiKeys.FETCH.parseResponse((short)6, buffer);
			FetchResponse fr = new FetchResponse(response);
			if (fr.isCorrect()) {
				byte[] value = fr.getRecord().getValue();
				if (value == null) {
					
					if ( isErrorOffsetRecovery )
						dataOffset.sendDefaultConsumerOffsetBack(consumeOffset, frontCon.getPassword());
					
					frontCon.write(NULL);
					return;
				}
				byte[] size = ProtoUtils.convertIntToByteArray(3);
				byte[] partitonArr = ProtoUtils.convertIntToByteArray(dataOffset.getPartition());
				byte[] partitonLength = ProtoUtils.convertIntToByteArray(partitonArr.length);
				byte[] offsetArr = String.valueOf(consumeOffset).getBytes();
				byte[] offsetLength = ProtoUtils.convertIntToByteArray(offsetArr.length);
				byte[] valueLenght = ProtoUtils.convertIntToByteArray(value.length);
				
				// 计算 bufferSize *3\r\n$1\r\n1\r\n$4\r\n2563\r\n$4\r\ntest\r\n 
				int bufferSize = 1 + size.length + 2 
						+ 1 + partitonLength.length + 2 + partitonArr.length + 2
						+ 1 + offsetLength.length + 2 + offsetArr.length + 2 
						+ 1 + valueLenght.length + 2 + value.length + 2;
				ByteBuffer bb = NetSystem.getInstance().getBufferPool().allocate(bufferSize);
				bb.put(ASTERISK).put(size).put(CRLF)
					.put(DOLLAR).put(partitonLength).put(CRLF).put(partitonArr).put(CRLF)
					.put(DOLLAR).put(offsetLength).put(CRLF).put(offsetArr).put(CRLF)
					.put(DOLLAR).put(valueLenght).put(CRLF).put(value).put(CRLF);
				
				frontCon.write(bb);
				
				// 消费offset超出范围
			} else if (fr.getFetchErr() != null && fr.getFetchErr().getCode() == Errors.OFFSET_OUT_OF_RANGE.code()) {
				
				if ( isErrorOffsetRecovery )
					dataOffset.sendDefaultConsumerOffsetBack(consumeOffset, frontCon.getPassword());
				
				frontCon.write(NULL);
				
				// 其他错误
			} else {
				
				if ( isErrorOffsetRecovery )
					dataOffset.sendDefaultConsumerOffsetBack(consumeOffset, frontCon.getPassword());
				
				StringBuffer sb = new StringBuffer();
				sb.append("-ERR ").append(fr.getErrorMessage()).append("\r\n");
				frontCon.write(sb.toString().getBytes());
			}
		}
	}
}
