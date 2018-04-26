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
package com.feeyo.kafka.codec;

import static com.feeyo.kafka.protocol.CommonFields.ERROR_CODE;
import static com.feeyo.kafka.protocol.CommonFields.THROTTLE_TIME_MS;
import static com.feeyo.kafka.protocol.types.Type.INT16;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.protocol.ApiKeys;

import com.feeyo.kafka.protocol.types.ArrayOf;
import com.feeyo.kafka.protocol.types.Field;
import com.feeyo.kafka.protocol.types.Schema;
import com.feeyo.kafka.protocol.types.Struct;

public class ApiVersionsResponse {
    private static final String API_VERSIONS_KEY_NAME = "api_versions";
    private static final String API_KEY_NAME = "api_key";
    private static final String MIN_VERSION_KEY_NAME = "min_version";
    private static final String MAX_VERSION_KEY_NAME = "max_version";

    private static final Schema API_VERSIONS_V0 = new Schema(
            new Field(API_KEY_NAME, INT16, "API key."),
            new Field(MIN_VERSION_KEY_NAME, INT16, "Minimum supported version."),
            new Field(MAX_VERSION_KEY_NAME, INT16, "Maximum supported version."));

    private static final Schema API_VERSIONS_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(API_VERSIONS_KEY_NAME, new ArrayOf(API_VERSIONS_V0), "API versions supported by the broker."));
    private static final Schema API_VERSIONS_RESPONSE_V1 = new Schema(
            ERROR_CODE,
            new Field(API_VERSIONS_KEY_NAME, new ArrayOf(API_VERSIONS_V0), "API versions supported by the broker."),
            THROTTLE_TIME_MS);

    public static Schema[] schemaVersions() {
        return new Schema[]{API_VERSIONS_RESPONSE_V0, API_VERSIONS_RESPONSE_V1};
    }
    
    private Errors error;
    private final Map<Short, ApiVersion> apiKeyToApiVersion;
    
    public ApiVersionsResponse(Struct struct) {
        this.error = Errors.forCode(struct.get(ERROR_CODE));
        List<ApiVersion> tempApiVersions = new ArrayList<>();
        for (Object apiVersionsObj : struct.getArray(API_VERSIONS_KEY_NAME)) {
            Struct apiVersionStruct = (Struct) apiVersionsObj;
            short apiKey = apiVersionStruct.getShort(API_KEY_NAME);
            short minVersion = apiVersionStruct.getShort(MIN_VERSION_KEY_NAME);
            short maxVersion = apiVersionStruct.getShort(MAX_VERSION_KEY_NAME);
            tempApiVersions.add(new ApiVersion(apiKey, minVersion, maxVersion));
        }
        this.apiKeyToApiVersion = buildApiKeyToApiVersion(tempApiVersions);
    }
    
    private Map<Short, ApiVersion> buildApiKeyToApiVersion(List<ApiVersion> apiVersions) {
        Map<Short, ApiVersion> tempApiIdToApiVersion = new HashMap<>();
        for (ApiVersion apiVersion : apiVersions) {
            tempApiIdToApiVersion.put(apiVersion.apiKey, apiVersion);
        }
        return tempApiIdToApiVersion;
    }
    
    public boolean isCorrect() {
		return error.getCode() == (short)0;
	}
	
	public String getErrorMessage() {
		return error.getMessage();
	}
	
	public Map<Short, ApiVersion> getApiKeyToApiVersion() {
		return apiKeyToApiVersion;
	}
    
    public static final class ApiVersion {
        public final short apiKey;
        public final short minVersion;
        public final short maxVersion;

        public ApiVersion(ApiKeys apiKey) {
            this(apiKey.id, apiKey.oldestVersion(), apiKey.latestVersion());
        }

        public ApiVersion(short apiKey, short minVersion, short maxVersion) {
            this.apiKey = apiKey;
            this.minVersion = minVersion;
            this.maxVersion = maxVersion;
        }

        @Override
		public String toString() {
			return "ApiVersion(" + "apiKey=" + apiKey + ", minVersion=" + minVersion + ", maxVersion= " + maxVersion + ")";
		}
    }

}
