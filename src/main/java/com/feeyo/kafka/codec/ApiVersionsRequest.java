package com.feeyo.kafka.codec;

import java.nio.ByteBuffer;

import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.Schema;
import com.feeyo.kafka.protocol.types.Struct;

public class ApiVersionsRequest {
	
    private static final Schema API_VERSIONS_REQUEST_V0 = new Schema();

    /* v1 request is the same as v0. Throttle time has been added to response */
    private static final Schema API_VERSIONS_REQUEST_V1 = API_VERSIONS_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{API_VERSIONS_REQUEST_V0, API_VERSIONS_REQUEST_V1};
    }

    private final Short unsupportedRequestVersion;
    private short version;

    public ApiVersionsRequest(short version) {
        this(version, null);
    }

    public ApiVersionsRequest(short version, Short unsupportedRequestVersion) {
    		this.version = version;
        this.unsupportedRequestVersion = unsupportedRequestVersion;
    }

    public ApiVersionsRequest(Struct struct, short version) {
        this(version, null);
    }

    public boolean hasUnsupportedRequestVersion() {
        return unsupportedRequestVersion != null;
    }

    protected Struct toStruct() {
        return new Struct(ApiKeys.API_VERSIONS.requestSchema(version()));
    }

    public static ApiVersionsRequest parse(ByteBuffer buffer, short version) {
        return new ApiVersionsRequest(ApiKeys.API_VERSIONS.parseRequest(version, buffer), version);
    }

	public short version() {
		return this.version;
	}
}
