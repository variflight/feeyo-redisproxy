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

/**
 * @see https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Responses
 * 
 * Requests all have the following format:
 * 
 * RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
 *  	ApiKey => int16
 * 		ApiVersion => int16
 * 		CorrelationId => int32
 * 		ClientId => string
 * 		RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest
 * 
 * @author yangtao
 *
 */
public class KafkaEncoder {
	
	// ACK
	// 0表示producer无需等待leader的确认
	// 1代表需要leader确认写入它的本地log并立即确认
	// -1代表所有的备份都完成后确认。
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
	
	// 用户可以使用他们喜欢的任何标识符，他们会被用在记录错误，监测统计信息等场景
	private static final String CLIENT_ID = "KafkaProxy";
	
	//
	public ByteBuffer encodeProduceRequest(RedisRequest request, int partition) {
		
		int offset = 0;
		String topic = new String( request.getArgs()[1] );
		byte[] key = request.getArgs()[1];
		byte[] value = request.getArgs()[request.getNumArgs() - 1];
		
		// offset 、topic 、 key、 value
		Record record = new Record(offset, topic, key, value);
		record.setTimestamp(TimeUtil.currentTimeMillis());
		record.setTimestampDelta(0);
		
		// version、 acks、timeout、 transactionalId、partitionId、 record
		short version = BrokerApiVersion.getProduceVersion();
		
		ProduceRequest pr = new ProduceRequest(version, ACKS, PRODUCE_WAIT_TIME_MS, null, partition, record);
		Struct body = pr.toStruct();
		
		// apiKeyId、 version、 clientId、correlation
		//这是一个用户提供的整数, 会被服务器原封不动地回传给客户端, 用于匹配客户机和服务器之间的请求和响应
		int correlationId = Utils.getCorrelationId();
		RequestHeader requestHeader = new RequestHeader(ApiKeys.PRODUCE.id, version, CLIENT_ID, correlationId);
		Struct header = requestHeader.toStruct();
		
		int headerBodySize = body.sizeOf() + header.sizeOf();
		int bufferSize = headerBodySize + LENGTH_BYTE_COUNT;
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( bufferSize );
		buffer.putInt( headerBodySize );
		header.writeTo(buffer);
		body.writeTo(buffer);
		return buffer;
	}
	
	//
	public ByteBuffer encodeFetchRequest(RedisRequest request, int partition, long offset, int maxBytes) {
		
		// topic
		TopicAndPartitionData<PartitionData> topicAndPartitionData = 
				new TopicAndPartitionData<PartitionData>(new String(request.getArgs()[1]));
		
		short version = BrokerApiVersion.getConsumerVersion();

		// version、replicaId、maxWait、minBytes、maxBytes、 isolationLevel、topicAndPartitionData、toForget、metadata
		int maxWait = maxBytes > 10240 ? CONSUME_WAIT_TIME_MS * 5 : CONSUME_WAIT_TIME_MS;
		FetchRequest fetchRequest = new FetchRequest(version, REPLICA_ID, maxWait, 
				MINBYTES, MAXBYTES, ISOLATION_LEVEL, topicAndPartitionData, null, FetchMetadata.LEGACY);
		
		// fetchOffset、logStartOffset、 maxBytes
		PartitionData partitionData = new PartitionData(offset, LOG_START_OFFSET, maxBytes);
		topicAndPartitionData.addData(partition, partitionData);
		
		// apiKeyId、 version、 clientId、correlation
		int correlationId = Utils.getCorrelationId();
		RequestHeader requestHeader = new RequestHeader(ApiKeys.FETCH.id, version, CLIENT_ID, correlationId);
		Struct header = requestHeader.toStruct();
		Struct body = fetchRequest.toStruct();
		
		int headerBodySize = body.sizeOf() + header.sizeOf();
		int bufferSize = headerBodySize + LENGTH_BYTE_COUNT;
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( bufferSize );
		buffer.putInt( headerBodySize );
		header.writeTo(buffer);
		body.writeTo(buffer);
		
		return buffer;
	}
	
	//
	public ByteBuffer encodeListOffsetRequest(RedisRequest request) {
		
		short version = BrokerApiVersion.getListOffsetsVersion();
		
		// apiKeyId、 version、 clientId、correlation
		int correlationId = Utils.getCorrelationId();
		RequestHeader requestHeader = new RequestHeader(ApiKeys.LIST_OFFSETS.id, version, CLIENT_ID, correlationId);

		//
		String topic = new String(request.getArgs()[1]);
		int partition = Integer.parseInt(new String(request.getArgs()[2]));
		long timestamp = Long.parseLong(new String(request.getArgs()[3])); 	// 根据时间查询最后此时间之后第一个点位。时间-1查询最大点位，-2查询最小点位。
		
		// version、 topic、 partition、 timestamp、 replicaId、 isolationLevel
		ListOffsetRequest listOffsetRequest = new ListOffsetRequest(version, topic, partition, timestamp, REPLICA_ID, ISOLATION_LEVEL);
		
		Struct header = requestHeader.toStruct();
		Struct body = listOffsetRequest.toStruct();
		
		int headerBodySize = body.sizeOf() + header.sizeOf();
		int bufferSize = headerBodySize + LENGTH_BYTE_COUNT;
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate( bufferSize );
		buffer.putInt( headerBodySize );
		header.writeTo(buffer);
		body.writeTo(buffer);
		
		return buffer;
	}

}