/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.feeyo.redis.kafka.codec;

import java.nio.ByteBuffer;

import com.feeyo.redis.kafka.protocol.ApiKeys;
import com.feeyo.redis.kafka.protocol.types.Schema;
import com.feeyo.redis.kafka.protocol.types.Struct;

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
