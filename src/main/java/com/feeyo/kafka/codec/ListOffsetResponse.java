package com.feeyo.kafka.codec;

import static com.feeyo.kafka.protocol.CommonFields.ERROR_CODE;
import static com.feeyo.kafka.protocol.CommonFields.PARTITION_ID;
import static com.feeyo.kafka.protocol.CommonFields.THROTTLE_TIME_MS;
import static com.feeyo.kafka.protocol.CommonFields.TOPIC_NAME;
import static com.feeyo.kafka.protocol.types.Type.INT64;

import com.feeyo.kafka.protocol.types.ArrayOf;
import com.feeyo.kafka.protocol.types.Field;
import com.feeyo.kafka.protocol.types.Schema;
import com.feeyo.kafka.protocol.types.Struct;

public class ListOffsetResponse {
	public static final long UNKNOWN_TIMESTAMP = -1L;
    public static final long UNKNOWN_OFFSET = -1L;

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String PARTITIONS_KEY_NAME = "partition_responses";

    /**
     * Possible error code:
     *
     *  UNKNOWN_TOPIC_OR_PARTITION (3)
     *  NOT_LEADER_FOR_PARTITION (6)
     *  UNSUPPORTED_FOR_MESSAGE_FORMAT (43)
     *  UNKNOWN (-1)
     */

    // This key is only used by ListOffsetResponse v0
    @Deprecated
    private static final String OFFSETS_KEY_NAME = "offsets";
    private static final String TIMESTAMP_KEY_NAME = "timestamp";
    private static final String OFFSET_KEY_NAME = "offset";

    private static final Schema LIST_OFFSET_RESPONSE_PARTITION_V0 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(OFFSETS_KEY_NAME, new ArrayOf(INT64), "A list of offsets."));

    private static final Schema LIST_OFFSET_RESPONSE_PARTITION_V1 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(TIMESTAMP_KEY_NAME, INT64, "The timestamp associated with the returned offset"),
            new Field(OFFSET_KEY_NAME, INT64, "offset found"));

    private static final Schema LIST_OFFSET_RESPONSE_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_PARTITION_V0)));

    private static final Schema LIST_OFFSET_RESPONSE_TOPIC_V1 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_PARTITION_V1)));

    private static final Schema LIST_OFFSET_RESPONSE_V0 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V0)));

    private static final Schema LIST_OFFSET_RESPONSE_V1 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V1)));
    private static final Schema LIST_OFFSET_RESPONSE_V2 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V1)));

    public static Schema[] schemaVersions() {
        return new Schema[] {LIST_OFFSET_RESPONSE_V0, LIST_OFFSET_RESPONSE_V1, LIST_OFFSET_RESPONSE_V2};
    }
    
    private String topic;
    private Errors error;
    private long timestamp = -1L;
    private long offset;
    
    public ListOffsetResponse(Struct struct) {
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                error = Errors.forCode(partitionResponse.get(ERROR_CODE));
                if (partitionResponse.hasField(OFFSETS_KEY_NAME)) {
                    Object[] offsets = partitionResponse.getArray(OFFSETS_KEY_NAME);
                    for (Object offset : offsets) {
                    		offset = (Long) offset;
                    }
                } else {
                    timestamp = partitionResponse.getLong(TIMESTAMP_KEY_NAME);
                    offset = partitionResponse.getLong(OFFSET_KEY_NAME);
                }
            }
        }
    }
    
    public long getOffset() {
		return offset;
	}

	public String getTopic() {
		return topic;
	}
		
	public boolean isCorrect() {
		return error.getCode() == (short)0;
	}
	
	public String getErrorMessage() {
		return error.getMessage();
	}

	public long getTimestamp() {
		return timestamp;
	}
	
}
