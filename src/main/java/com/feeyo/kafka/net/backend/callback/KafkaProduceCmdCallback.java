package com.feeyo.kafka.net.backend.callback;

import java.nio.ByteBuffer;

import com.feeyo.kafka.codec.ProduceResponse;
import com.feeyo.kafka.net.backend.broker.BrokerApiVersion;
import com.feeyo.kafka.net.backend.broker.offset.BrokerOffsetService;
import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Struct;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.util.ProtoUtils;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;

public class KafkaProduceCmdCallback extends KafkaCmdCallback {
	
	private int partition;
	
	public KafkaProduceCmdCallback(int partition) {
		this.partition = partition;
	}
	
	@Override
	public void parseResponseBody(BackendConnection conn, ByteBuffer buffer) {
		
		short version = BrokerApiVersion.getProduceVersion();
		Struct response = ApiKeys.PRODUCE.parseResponse(version, buffer);
		ProduceResponse pr = new ProduceResponse(response);
		
		//
		RedisFrontConnection frontCon = getFrontCon( conn );
		
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
