package com.feeyo.kafka.net.backend.callback;

import java.nio.ByteBuffer;

import com.feeyo.kafka.codec.ListOffsetResponse;
import com.feeyo.kafka.net.backend.broker.BrokerApiVersion;
import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Struct;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.util.ProtoUtils;
import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.front.RedisFrontConnection;

public class KafkaOffsetCmdCallback extends KafkaCmdCallback {

	@Override
	public void parseResponseBody(BackendConnection conn, ByteBuffer byteBuff) {

		short version = BrokerApiVersion.getListOffsetsVersion();
		Struct response = ApiKeys.LIST_OFFSETS.parseResponse(version, byteBuff);
		ListOffsetResponse lor = new ListOffsetResponse(response);
		
		// 1k 的 buffer 肯定够用
		ByteBuffer responseBuf = NetSystem.getInstance().getBufferPool().allocate(1024);
		if (lor.isCorrect()) {
			
			byte[] size = ProtoUtils.convertIntToByteArray(OFFSET_RESPONSE_SIZE);
			byte[] offsetArr = String.valueOf(lor.getOffset()).getBytes();
			byte[] offsetLength = ProtoUtils.convertIntToByteArray(offsetArr.length);
			byte[] timestampArr = String.valueOf(lor.getTimestamp()).getBytes();
			byte[] timestampLength = ProtoUtils.convertIntToByteArray(timestampArr.length);
			
			//
			responseBuf.put(ASTERISK).put(size).put(CRLF);
			responseBuf.put(DOLLAR).put(offsetLength).put(CRLF).put(offsetArr).put(CRLF);
			responseBuf.put(DOLLAR).put(timestampLength).put(CRLF).put(timestampArr).put(CRLF);
			
		} else {
			
			byte[] size = ProtoUtils.convertIntToByteArray(1);
			byte[] msg = lor.getErrorMessage().getBytes();
			byte[] msgLen = ProtoUtils.convertIntToByteArray(msg.length);

			responseBuf.put(ASTERISK).put(size).put(CRLF);
			responseBuf.put(DOLLAR).put(msgLen).put(CRLF).put(msg).put(CRLF);
		}
		
		// response to front
		RedisFrontConnection frontCon = getFrontCon( conn );
		frontCon.write(responseBuf);
	}	
}