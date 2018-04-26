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

import static com.feeyo.kafka.protocol.CommonFields.PARTITION_ID;
import static com.feeyo.kafka.protocol.CommonFields.TOPIC_NAME;
import static com.feeyo.kafka.protocol.types.Type.INT32;
import static com.feeyo.kafka.protocol.types.Type.INT64;
import static com.feeyo.kafka.protocol.types.Type.INT8;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.ArrayOf;
import com.feeyo.kafka.protocol.types.Field;
import com.feeyo.kafka.protocol.types.Schema;
import com.feeyo.kafka.protocol.types.Struct;
import com.feeyo.kafka.protocol.types.Type;

public class FetchRequest {
    public static final int CONSUMER_REPLICA_ID = -1;
    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String MAX_WAIT_KEY_NAME = "max_wait_time";
    private static final String MIN_BYTES_KEY_NAME = "min_bytes";
    private static final String ISOLATION_LEVEL_KEY_NAME = "isolation_level";
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String FORGOTTEN_TOPICS_DATA = "forgetten_topics_data";

    // request and partition level name
    private static final String MAX_BYTES_KEY_NAME = "max_bytes";

    // topic level field names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String FETCH_OFFSET_KEY_NAME = "fetch_offset";
    private static final String LOG_START_OFFSET_KEY_NAME = "log_start_offset";

    private static final Schema FETCH_REQUEST_PARTITION_V0 = new Schema(
            PARTITION_ID,
            new Field(FETCH_OFFSET_KEY_NAME, INT64, "Message offset."),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to fetch."));

    // FETCH_REQUEST_PARTITION_V5 added log_start_offset field - the earliest available offset of partition data that can be consumed.
    private static final Schema FETCH_REQUEST_PARTITION_V5 = new Schema(
            PARTITION_ID,
            new Field(FETCH_OFFSET_KEY_NAME, INT64, "Message offset."),
            new Field(LOG_START_OFFSET_KEY_NAME, INT64, "Earliest available offset of the follower replica. " +
                            "The field is only used when request is sent by follower. "),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to fetch."));

    private static final Schema FETCH_REQUEST_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_REQUEST_PARTITION_V0), "Partitions to fetch."));

    private static final Schema FETCH_REQUEST_TOPIC_V5 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_REQUEST_PARTITION_V5), "Partitions to fetch."));

    private static final Schema FETCH_REQUEST_V0 = new Schema(
            new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field(MAX_WAIT_KEY_NAME, INT32, "Maximum time in ms to wait for the response."),
            new Field(MIN_BYTES_KEY_NAME, INT32, "Minimum bytes to accumulate in the response."),
            new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V0), "Topics to fetch."));

    // The V1 Fetch Request body is the same as V0.
    // Only the version number is incremented to indicate a newer client
    private static final Schema FETCH_REQUEST_V1 = FETCH_REQUEST_V0;
    // The V2 Fetch Request body is the same as V1.
    // Only the version number is incremented to indicate the client support message format V1 which uses
    // relative offset and has timestamp.
    private static final Schema FETCH_REQUEST_V2 = FETCH_REQUEST_V1;
    // Fetch Request V3 added top level max_bytes field - the total size of partition data to accumulate in response.
    // The partition ordering is now relevant - partitions will be processed in order they appear in request.
    private static final Schema FETCH_REQUEST_V3 = new Schema(
            new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field(MAX_WAIT_KEY_NAME, INT32, "Maximum time in ms to wait for the response."),
            new Field(MIN_BYTES_KEY_NAME, INT32, "Minimum bytes to accumulate in the response."),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                    "if the first message in the first non-empty partition of the fetch is larger than this " +
                    "value, the message will still be returned to ensure that progress can be made."),
            new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V0), "Topics to fetch in the order provided."));

    // The V4 Fetch Request adds the fetch isolation level and exposes magic v2 (via the response).
    private static final Schema FETCH_REQUEST_V4 = new Schema(
            new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field(MAX_WAIT_KEY_NAME, INT32, "Maximum time in ms to wait for the response."),
            new Field(MIN_BYTES_KEY_NAME, INT32, "Minimum bytes to accumulate in the response."),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                    "if the first message in the first non-empty partition of the fetch is larger than this " +
                    "value, the message will still be returned to ensure that progress can be made."),
            new Field(ISOLATION_LEVEL_KEY_NAME, INT8, "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED " +
                    "(isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), " +
                    "non-transactional and COMMITTED transactional records are visible. To be more concrete, " +
                    "READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), " +
                    "and enables the inclusion of the list of aborted transactions in the result, which allows " +
                    "consumers to discard ABORTED transactional records"),
            new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V0), "Topics to fetch in the order provided."));

    // FETCH_REQUEST_V5 added a per-partition log_start_offset field - the earliest available offset of partition data that can be consumed.
    private static final Schema FETCH_REQUEST_V5 = new Schema(
            new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field(MAX_WAIT_KEY_NAME, INT32, "Maximum time in ms to wait for the response."),
            new Field(MIN_BYTES_KEY_NAME, INT32, "Minimum bytes to accumulate in the response."),
            new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
                    "if the first message in the first non-empty partition of the fetch is larger than this " +
                    "value, the message will still be returned to ensure that progress can be made."),
            new Field(ISOLATION_LEVEL_KEY_NAME, INT8, "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED " +
                    "(isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), " +
                    "non-transactional and COMMITTED transactional records are visible. To be more concrete, " +
                    "READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), " +
                    "and enables the inclusion of the list of aborted transactions in the result, which allows " +
                    "consumers to discard ABORTED transactional records"),
            new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V5), "Topics to fetch in the order provided."));

    /**
     * The body of FETCH_REQUEST_V6 is the same as FETCH_REQUEST_V5.
     * The version number is bumped up to indicate that the client supports KafkaStorageException.
     * The KafkaStorageException will be translated to NotLeaderForPartitionException in the response if version <= 5
     */
    private static final Schema FETCH_REQUEST_V6 = FETCH_REQUEST_V5;

    // FETCH_REQUEST_V7 added incremental fetch requests.
    public static final Field.Int32 SESSION_ID = new Field.Int32("session_id", "The fetch session ID");
    public static final Field.Int32 EPOCH = new Field.Int32("epoch", "The fetch epoch");

    private static final Schema FORGOTTEN_TOPIC_DATA = new Schema(
        TOPIC_NAME,
        new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32),
            "Partitions to remove from the fetch session."));

    private static final Schema FETCH_REQUEST_V7 = new Schema(
        new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
        new Field(MAX_WAIT_KEY_NAME, INT32, "Maximum time in ms to wait for the response."),
        new Field(MIN_BYTES_KEY_NAME, INT32, "Minimum bytes to accumulate in the response."),
        new Field(MAX_BYTES_KEY_NAME, INT32, "Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, " +
            "if the first message in the first non-empty partition of the fetch is larger than this " +
            "value, the message will still be returned to ensure that progress can be made."),
        new Field(ISOLATION_LEVEL_KEY_NAME, INT8, "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED " +
            "(isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), " +
            "non-transactional and COMMITTED transactional records are visible. To be more concrete, " +
            "READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), " +
            "and enables the inclusion of the list of aborted transactions in the result, which allows " +
            "consumers to discard ABORTED transactional records"),
        SESSION_ID,
        EPOCH,
        new Field(TOPICS_KEY_NAME, new ArrayOf(FETCH_REQUEST_TOPIC_V5), "Topics to fetch in the order provided."),
        new Field(FORGOTTEN_TOPICS_DATA, new ArrayOf(FORGOTTEN_TOPIC_DATA), "Topics to remove from the fetch session."));

    public static Schema[] schemaVersions() {
        return new Schema[]{FETCH_REQUEST_V0, FETCH_REQUEST_V1, FETCH_REQUEST_V2, FETCH_REQUEST_V3, FETCH_REQUEST_V4,
            FETCH_REQUEST_V5, FETCH_REQUEST_V6, FETCH_REQUEST_V7};
    };

    // default values for older versions where a request level limit did not exist
    public static final int DEFAULT_RESPONSE_MAX_BYTES = Integer.MAX_VALUE;
    public static final long INVALID_LOG_START_OFFSET = -1L;

    private final short version;
    private final int replicaId;
    private final int maxWait;
    private final int minBytes;
    private final int maxBytes;
    private final byte isolationLevel;
    
    private final TopicAndPartitionData<PartitionData> topicAndPartitionData;

    public static final class PartitionData {
        public final long fetchOffset;
        public final long logStartOffset;
        public final int maxBytes;

        public PartitionData(long fetchOffset, long logStartOffset, int maxBytes) {
            this.fetchOffset = fetchOffset;
            this.logStartOffset = logStartOffset;
            this.maxBytes = maxBytes;
        }

        @Override
        public String toString() {
            return "(offset=" + fetchOffset + ", logStartOffset=" + logStartOffset + ", maxBytes=" + maxBytes + ")";
        }

        @Override
        public int hashCode() {
            return Objects.hash(fetchOffset, logStartOffset, maxBytes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionData that = (PartitionData) o;
            return Objects.equals(fetchOffset, that.fetchOffset) &&
                Objects.equals(logStartOffset, that.logStartOffset) &&
                Objects.equals(maxBytes, that.maxBytes);
        }
    }

   public static final class TopicAndPartitionData<T> {
        public final String topic;
        public final LinkedHashMap<Integer, T> partitions;

        public TopicAndPartitionData(String topic) {
            this.topic = topic;
            this.partitions = new LinkedHashMap<>();
        }
        
        public void addData(int key, T value) {
        		partitions.put(key, value);
        }
    }


   /**
    * 
    * @param version
    * @param replicaId
    * @param maxWait
    * @param minBytes
    * @param maxBytes  
    * @param isolationLevel
    * @param topicAndPartitionData
    */
    public FetchRequest(short version, int replicaId, int maxWait, int minBytes, int maxBytes,
                        byte isolationLevel, TopicAndPartitionData<PartitionData> topicAndPartitionData) {
    		this.version = version;
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.isolationLevel = isolationLevel;
        this.topicAndPartitionData = topicAndPartitionData;
    }

    public int replicaId() {
        return replicaId;
    }

    public int maxWait() {
        return maxWait;
    }

    public int minBytes() {
        return minBytes;
    }

    public int maxBytes() {
        return maxBytes;
    }

    public Struct toStruct() {
        Struct struct = new Struct(ApiKeys.FETCH.requestSchema(version));

        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        struct.set(MAX_WAIT_KEY_NAME, maxWait);
        struct.set(MIN_BYTES_KEY_NAME, minBytes);
        if (struct.hasField(MAX_BYTES_KEY_NAME))
            struct.set(MAX_BYTES_KEY_NAME, maxBytes);
        if (struct.hasField(ISOLATION_LEVEL_KEY_NAME))
            struct.set(ISOLATION_LEVEL_KEY_NAME, isolationLevel);

        List<Struct> topicArray = new ArrayList<>();
        Struct topicData = struct.instance(TOPICS_KEY_NAME);
        topicData.set(TOPIC_NAME, topicAndPartitionData.topic);
        List<Struct> partitionArray = new ArrayList<>();
        for (Map.Entry<Integer, PartitionData> partitionEntry : topicAndPartitionData.partitions.entrySet()) {
            PartitionData fetchPartitionData = partitionEntry.getValue();
            Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
            partitionData.set(PARTITION_ID, partitionEntry.getKey());
            partitionData.set(FETCH_OFFSET_KEY_NAME, fetchPartitionData.fetchOffset);
            if (partitionData.hasField(LOG_START_OFFSET_KEY_NAME))
                partitionData.set(LOG_START_OFFSET_KEY_NAME, fetchPartitionData.logStartOffset);
            partitionData.set(MAX_BYTES_KEY_NAME, fetchPartitionData.maxBytes);
            partitionArray.add(partitionData);
        }
        topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
        topicArray.add(topicData);
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        return struct;
    }
}
