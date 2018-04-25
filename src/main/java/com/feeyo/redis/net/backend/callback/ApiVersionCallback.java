package com.feeyo.redis.net.backend.callback;

import java.nio.ByteBuffer;

import com.feeyo.redis.config.kafka.MetaData;
import com.feeyo.redis.kafka.codec.ApiVersionsResponse;
import com.feeyo.redis.kafka.protocol.ApiKeys;
import com.feeyo.redis.kafka.protocol.types.Struct;

public class ApiVersionCallback extends KafkaCmdCallback {

	@Override
	public void handle(ByteBuffer buffer) {
		Struct response = ApiKeys.API_VERSIONS.parseResponse((short) 1, buffer);
		ApiVersionsResponse ar = new ApiVersionsResponse(response);
		if (ar.isCorrect()) {
			MetaData.setApiVersions( ar.getApiKeyToApiVersion() );
		}
	}
}
