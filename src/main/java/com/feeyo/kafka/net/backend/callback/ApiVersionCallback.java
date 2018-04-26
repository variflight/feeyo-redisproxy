package com.feeyo.kafka.net.backend.callback;

import java.nio.ByteBuffer;

import com.feeyo.kafka.codec.ApiVersionsResponse;
import com.feeyo.kafka.config.MetaData;
import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Struct;

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
