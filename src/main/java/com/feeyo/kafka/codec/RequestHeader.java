package com.feeyo.kafka.codec;

import static com.feeyo.kafka.protocol.types.Type.INT16;
import static com.feeyo.kafka.protocol.types.Type.INT32;
import static com.feeyo.kafka.protocol.types.Type.NULLABLE_STRING;

import com.feeyo.kafka.protocol.types.Field;
import com.feeyo.kafka.protocol.types.Schema;
import com.feeyo.kafka.protocol.types.Struct;

public class RequestHeader {
	
	private static final String API_KEY_FIELD_NAME = "api_key";
	private static final String API_VERSION_FIELD_NAME = "api_version";
	private static final String CLIENT_ID_FIELD_NAME = "client_id";
	private static final String CORRELATION_ID_FIELD_NAME = "correlation_id";

	public static final Schema SCHEMA = new Schema(
			new Field(API_KEY_FIELD_NAME, INT16, "The id of the request type."),
			new Field(API_VERSION_FIELD_NAME, INT16, "The version of the API."),
			new Field(CORRELATION_ID_FIELD_NAME, INT32, "A user-supplied integer value that will be passed back with the response"),
			new Field(CLIENT_ID_FIELD_NAME, NULLABLE_STRING, "A user specified identifier for the client making the request.", ""));


	private final short apiVersion;
	private final String clientId;
	
	// 客户端标示，自增
	private final int correlationId;
	private final short apiKeyId;

	public RequestHeader(short apiKeyId, short version, String clientId, int correlation) {
		this.apiKeyId = apiKeyId;
		this.apiVersion = version;
		this.clientId = clientId;
		this.correlationId = correlation;
	}

	public Struct toStruct() {
		Struct struct = new Struct(SCHEMA);
		struct.set(API_KEY_FIELD_NAME, apiKeyId);
		struct.set(API_VERSION_FIELD_NAME, apiVersion);

		// only v0 of the controlled shutdown request is missing the clientId
		if (struct.hasField(CLIENT_ID_FIELD_NAME))
			struct.set(CLIENT_ID_FIELD_NAME, clientId);
		struct.set(CORRELATION_ID_FIELD_NAME, correlationId);
		return struct;
	}

	public short apiVersion() {
		return apiVersion;
	}

	public String clientId() {
		return clientId;
	}

	public int correlationId() {
		return correlationId;
	}


	@Override
	public String toString() {
		return "RequestHeader(" + "apiVersion=" + apiVersion + ", clientId=" + clientId
				+ ", correlationId=" + correlationId + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		RequestHeader that = (RequestHeader) o;
		return apiVersion == that.apiVersion && correlationId == that.correlationId
				&& (clientId == null ? that.clientId == null : clientId.equals(that.clientId));
	}

	@Override
	public int hashCode() {
		int result = (int) apiVersion;
		result = 31 * result + (clientId != null ? clientId.hashCode() : 0);
		result = 31 * result + correlationId;
		return result;
	}

}
