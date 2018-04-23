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

import static com.feeyo.redis.kafka.protocol.CommonFields.NULLABLE_TRANSACTIONAL_ID;
import static com.feeyo.redis.kafka.protocol.CommonFields.PARTITION_ID;
import static com.feeyo.redis.kafka.protocol.CommonFields.TOPIC_NAME;
import static com.feeyo.redis.kafka.protocol.types.Type.INT16;
import static com.feeyo.redis.kafka.protocol.types.Type.INT32;
import static com.feeyo.redis.kafka.protocol.types.Type.RECORDS;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.redis.kafka.protocol.ApiKeys;
import com.feeyo.redis.kafka.protocol.CommonFields;
import com.feeyo.redis.kafka.protocol.types.ArrayOf;
import com.feeyo.redis.kafka.protocol.types.Field;
import com.feeyo.redis.kafka.protocol.types.Schema;
import com.feeyo.redis.kafka.protocol.types.Struct;

public class ProduceRequest {
    private static final String ACKS_KEY_NAME = "acks";
    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String TOPIC_DATA_KEY_NAME = "topic_data";

    // topic level field names
    private static final String PARTITION_DATA_KEY_NAME = "data";

    // partition level field names
    private static final String RECORD_SET_KEY_NAME = "record_set";

    private final Record record;

    private static final Schema TOPIC_PRODUCE_DATA_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITION_DATA_KEY_NAME, new ArrayOf(new Schema(
                    PARTITION_ID,
                    new Field(RECORD_SET_KEY_NAME, RECORDS)))));

    private static final Schema PRODUCE_REQUEST_V0 = new Schema(
            new Field(ACKS_KEY_NAME, INT16, "The number of acknowledgments the producer requires the leader to have " +
                    "received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for " +
                    "only the leader and -1 for the full ISR."),
            new Field(TIMEOUT_KEY_NAME, INT32, "The time to await a response in ms."),
            new Field(TOPIC_DATA_KEY_NAME, new ArrayOf(TOPIC_PRODUCE_DATA_V0)));

    /**
     * The body of PRODUCE_REQUEST_V1 is the same as PRODUCE_REQUEST_V0.
     * The version number is bumped up to indicate that the client supports quota throttle time field in the response.
     */
    private static final Schema PRODUCE_REQUEST_V1 = PRODUCE_REQUEST_V0;
    /**
     * The body of PRODUCE_REQUEST_V2 is the same as PRODUCE_REQUEST_V1.
     * The version number is bumped up to indicate that message format V1 is used which has relative offset and
     * timestamp.
     */
    private static final Schema PRODUCE_REQUEST_V2 = PRODUCE_REQUEST_V1;

    // Produce request V3 adds the transactional id which is used for authorization when attempting to write
    // transactional data. This version also adds support for message format V2.
    private static final Schema PRODUCE_REQUEST_V3 = new Schema(
            CommonFields.NULLABLE_TRANSACTIONAL_ID,
            new Field(ACKS_KEY_NAME, INT16, "The number of acknowledgments the producer requires the leader to have " +
                    "received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 " +
                    "for only the leader and -1 for the full ISR."),
            new Field(TIMEOUT_KEY_NAME, INT32, "The time to await a response in ms."),
            new Field(TOPIC_DATA_KEY_NAME, new ArrayOf(TOPIC_PRODUCE_DATA_V0)));

    /**
     * The body of PRODUCE_REQUEST_V4 is the same as PRODUCE_REQUEST_V3.
     * The version number is bumped up to indicate that the client supports KafkaStorageException.
     * The KafkaStorageException will be translated to NotLeaderForPartitionException in the response if version <= 3
     */
    private static final Schema PRODUCE_REQUEST_V4 = PRODUCE_REQUEST_V3;

    /**
     * The body of the PRODUCE_REQUEST_V5 is the same as PRODUCE_REQUEST_V4.
     * The version number is bumped since the PRODUCE_RESPONSE_V5 includes an additional partition level
     * field: the log_start_offset.
     */
    private static final Schema PRODUCE_REQUEST_V5 = PRODUCE_REQUEST_V4;


    public static Schema[] schemaVersions() {
        return new Schema[] {PRODUCE_REQUEST_V0, PRODUCE_REQUEST_V1, PRODUCE_REQUEST_V2, PRODUCE_REQUEST_V3,
            PRODUCE_REQUEST_V4, PRODUCE_REQUEST_V5};
    }


    private final short acks;
    private final int timeout;
    private final String transactionalId;
    private final int partitionId;

    private boolean transactional = false;
    private boolean idempotent = false;

    public ProduceRequest(short version, short acks, int timeout, String transactionalId, int partitionId, Record record) {
        this.acks = acks;
        this.timeout = timeout;
        this.transactionalId = transactionalId;
        this.partitionId = partitionId;
        this.record = record;
    }

    /**
     * Visible for testing.
     */
    public Struct toStruct() {
        Struct struct = new Struct(ApiKeys.PRODUCE.requestSchema((short)3));
        struct.set(ACKS_KEY_NAME, acks);
        struct.set(TIMEOUT_KEY_NAME, timeout);
        struct.setIfExists(NULLABLE_TRANSACTIONAL_ID, transactionalId);
        
        List<Struct> topicDatas = new ArrayList<>(1);
        List<Struct> partitionArray = new ArrayList<>();
        Struct topicData = struct.instance(TOPIC_DATA_KEY_NAME);
        Struct part = topicData.instance(PARTITION_DATA_KEY_NAME)
        			// PARTITION_ID
                .set(PARTITION_ID, partitionId)
                .set(RECORD_SET_KEY_NAME, record);
        partitionArray.add(part);
        topicData.set(PARTITION_DATA_KEY_NAME, partitionArray.toArray());
        topicData.set(TOPIC_NAME, record.getTopic());
        topicDatas.add(topicData);

        struct.set(TOPIC_DATA_KEY_NAME, topicDatas.toArray());
        return struct;
    }



    public short acks() {
        return acks;
    }

    public int timeout() {
        return timeout;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public boolean isTransactional() {
        return transactional;
    }

    public boolean isIdempotent() {
        return idempotent;
    }

}
