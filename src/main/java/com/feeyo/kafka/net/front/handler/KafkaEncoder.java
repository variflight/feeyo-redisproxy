package com.feeyo.kafka.net.front.handler;

import java.nio.ByteBuffer;

import com.feeyo.kafka.codec.FetchMetadata;
import com.feeyo.kafka.codec.FetchRequest;
import com.feeyo.kafka.codec.IsolationLevel;
import com.feeyo.kafka.codec.ListOffsetRequest;
import com.feeyo.kafka.codec.ProduceRequest;
import com.feeyo.kafka.codec.Record;
import com.feeyo.kafka.codec.RequestHeader;
import com.feeyo.kafka.codec.FetchRequest.PartitionData;
import com.feeyo.kafka.codec.FetchRequest.TopicAndPartitionData;
import com.feeyo.kafka.net.backend.broker.BrokerApiVersion;
import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Struct;
import com.feeyo.kafka.util.Utils;
import com.feeyo.net.codec.redis.RedisRequest;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.util.TimeUtil;

public class KafkaEncoder {
	
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
	
	
	//
	public ByteBuffer encodeProduce(RedisRequest request, int partition) {
		
		short version = BrokerApiVersion.getProduceVersion();
		
		Record record = new Record(0, new String(request.getArgs()[1]), request.getArgs()[1], request.getArgs()[2]);
		record.setTimestamp(TimeUtil.currentTimeMillis());
		record.setTimestampDelta(0);
		ProduceRequest pr = new ProduceRequest(version, ACKS, PRODUCE_WAIT_TIME_MS, null, partition, record);
		Struct body = pr.toStruct();
		
		RequestHeader requestHeader = new RequestHeader(ApiKeys.PRODUCE.id, version, Thread.currentThread().getName(), Utils.getCorrelationId());
		Struct header = requestHeader.toStruct();
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( body.sizeOf() + header.sizeOf() + LENGTH_BYTE_COUNT);
		buffer.putInt(body.sizeOf() + header.sizeOf());
		header.writeTo(buffer);
		body.writeTo(buffer);
		return buffer;
	}
	
	//
	public ByteBuffer encodeConsumer(RedisRequest request, int partition, long offset, int maxBytes) {
		
		short version = BrokerApiVersion.getConsumerVersion();
		
		TopicAndPartitionData<PartitionData> topicAndPartitionData = 
				new TopicAndPartitionData<PartitionData>(new String(request.getArgs()[1]));
		
		FetchRequest fetchRequest = new FetchRequest(version, REPLICA_ID, maxBytes > 10240 ? CONSUME_WAIT_TIME_MS * 5 : CONSUME_WAIT_TIME_MS, 
				MINBYTES, MAXBYTES, ISOLATION_LEVEL, topicAndPartitionData, null, FetchMetadata.LEGACY);
		
		PartitionData pd = new PartitionData(offset, LOG_START_OFFSET, maxBytes);
		topicAndPartitionData.addData(partition, pd);
		
		RequestHeader requestHeader = new RequestHeader(ApiKeys.FETCH.id, version, 
				Thread.currentThread().getName(), Utils.getCorrelationId());
		Struct header = requestHeader.toStruct();
		Struct body = fetchRequest.toStruct();
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate(body.sizeOf() + header.sizeOf() + LENGTH_BYTE_COUNT);
		buffer.putInt(body.sizeOf() + header.sizeOf());
		header.writeTo(buffer);
		body.writeTo(buffer);
		
		return buffer;
	}
	
	//
	public ByteBuffer encodeListOffsets(RedisRequest request) {
		
		short version = BrokerApiVersion.getListOffsetsVersion();
		
		RequestHeader requestHeader = new RequestHeader(ApiKeys.LIST_OFFSETS.id, version, Thread.currentThread().getName(), Utils.getCorrelationId());
		
		String topic = new String(request.getArgs()[1]);
		int partition = Integer.parseInt(new String(request.getArgs()[2]));
		// 根据时间查询最后此时间之后第一个点位。时间-1查询最大点位，-2查询最小点位。
		long timestamp = Long.parseLong(new String(request.getArgs()[3]));
		ListOffsetRequest listOffsetRequest = new ListOffsetRequest(version, topic, partition, timestamp, REPLICA_ID, ISOLATION_LEVEL);
		
		Struct header = requestHeader.toStruct();
		Struct body = listOffsetRequest.toStruct();
		
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate(body.sizeOf() + header.sizeOf() + LENGTH_BYTE_COUNT);
		buffer.putInt(body.sizeOf() + header.sizeOf());
		header.writeTo(buffer);
		body.writeTo(buffer);
		
		return buffer;
	}

}
