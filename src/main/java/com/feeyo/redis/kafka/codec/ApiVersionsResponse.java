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

import static com.feeyo.redis.kafka.protocol.CommonFields.ERROR_CODE;
import static com.feeyo.redis.kafka.protocol.CommonFields.THROTTLE_TIME_MS;
import static com.feeyo.redis.kafka.protocol.types.Type.INT16;

import com.feeyo.redis.kafka.protocol.types.ArrayOf;
import com.feeyo.redis.kafka.protocol.types.Field;
import com.feeyo.redis.kafka.protocol.types.Schema;

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


}
