package com.feeyo.kafka.net.backend.callback;

import java.nio.ByteBuffer;
import java.util.List;

import com.feeyo.kafka.codec.Errors;
import com.feeyo.kafka.codec.FetchResponse;
import com.feeyo.kafka.codec.Record;
import com.feeyo.kafka.net.backend.broker.BrokerApiVersion;
import com.feeyo.kafka.net.backend.broker.offset.BrokerOffsetService;
import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Struct;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;
import com.feeyo.util.ProtoUtils;

public class KafkaConsumerCmdCallback extends KafkaCmdCallback {
	
	private String topic;
	private long consumeOffset;
	private int partition;
	
	// 消费失败是否把消费点位归还（指定点位消费时，不需要归还）
	private boolean isErrorOffsetRecovery = true;
	
	public KafkaConsumerCmdCallback(String topic, int partition, long offset, boolean isErrorOffsetRecovery) {
		this.topic = topic;
		this.partition = partition;
		this.consumeOffset = offset;
		this.isErrorOffsetRecovery = isErrorOffsetRecovery;
	}
	
	@Override
	public void parseResponseBody(BackendConnection conn, ByteBuffer respBuffer) {
		
		//
		RedisFrontConnection frontCon = getFrontCon( conn );
		
		short version = BrokerApiVersion.getConsumerVersion();
		
		Struct response = ApiKeys.FETCH.parseResponse(version, respBuffer);
		FetchResponse fr = new FetchResponse(response);
		if (fr.isCorrect()) {
			List<Record> records = fr.getRecords();
			if (records == null || records.isEmpty()) {
				if ( isErrorOffsetRecovery )
					returnConsumerOffset(frontCon.getPassword(), topic, partition, consumeOffset);
				
				frontCon.write(NULL);
				return;
			}
			
			byte[] size = ProtoUtils.convertIntToByteArray(CONSUMER_RESPONSE_SIZE * records.size());
			
			for (int i = 0;i<records.size();i++) {
				Record record = records.get(i);
				byte[] value = record.getValue();
				
				if (value == null) {
					if ( isErrorOffsetRecovery )
						returnConsumerOffset(frontCon.getPassword(), topic, partition, consumeOffset);
					
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
				returnConsumerOffset(frontCon.getPassword(), topic, partition, consumeOffset);
			
			frontCon.write(NULL);
			
		// 其他错误
		} else {
			
			if ( isErrorOffsetRecovery )
				returnConsumerOffset(frontCon.getPassword(), topic, partition, consumeOffset);
			
			StringBuffer sb = new StringBuffer();
			sb.append("-ERR ").append(fr.getErrorMessage()).append("\r\n");
			frontCon.write(sb.toString().getBytes());
		}
	}
	
	private void returnConsumerOffset(String password, String topic, int partition, long offset) {
		BrokerOffsetService.INSTANCE().returnOffset(password, topic, partition, offset);
	}
}
