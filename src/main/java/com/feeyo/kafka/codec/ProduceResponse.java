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

/**
 * This wrapper supports both v0 and v1 of ProduceResponse.
 */
public class ProduceResponse {

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String PARTITION_RESPONSES_KEY_NAME = "partition_responses";

    public static final long INVALID_OFFSET = -1L;

    /**
     * Possible error code:
     *
     * CORRUPT_MESSAGE (2)
     * UNKNOWN_TOPIC_OR_PARTITION (3)
     * NOT_LEADER_FOR_PARTITION (6)
     * MESSAGE_TOO_LARGE (10)
     * INVALID_TOPIC (17)
     * RECORD_LIST_TOO_LARGE (18)
     * NOT_ENOUGH_REPLICAS (19)
     * NOT_ENOUGH_REPLICAS_AFTER_APPEND (20)
     * INVALID_REQUIRED_ACKS (21)
     * TOPIC_AUTHORIZATION_FAILED (29)
     * UNSUPPORTED_FOR_MESSAGE_FORMAT (43)
     * INVALID_PRODUCER_EPOCH (47)
     * CLUSTER_AUTHORIZATION_FAILED (31)
     * TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)
     */

    private static final String BASE_OFFSET_KEY_NAME = "base_offset";
    private static final String LOG_APPEND_TIME_KEY_NAME = "log_append_time";
    private static final String LOG_START_OFFSET_KEY_NAME = "log_start_offset";

    private static final Field.Int64 LOG_START_OFFSET_FIELD = new Field.Int64(LOG_START_OFFSET_KEY_NAME,
            "The start offset of the log at the time this produce response was created", INVALID_OFFSET);

    private static final Schema PRODUCE_RESPONSE_V0 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE,
                            new Field(BASE_OFFSET_KEY_NAME, INT64))))))));

    private static final Schema PRODUCE_RESPONSE_V1 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE,
                            new Field(BASE_OFFSET_KEY_NAME, INT64))))))),
            THROTTLE_TIME_MS);

    /**
     * PRODUCE_RESPONSE_V2 added a timestamp field in the per partition response status.
     * The timestamp is log append time if the topic is configured to use log append time. Or it is NoTimestamp when create
     * time is used for the topic.
     */
    private static final Schema PRODUCE_RESPONSE_V2 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE,
                            new Field(BASE_OFFSET_KEY_NAME, INT64),
                            new Field(LOG_APPEND_TIME_KEY_NAME, INT64, "The timestamp returned by broker after appending " +
                                    "the messages. If CreateTime is used for the topic, the timestamp will be -1. " +
                                    "If LogAppendTime is used for the topic, the timestamp will be " +
                                    "the broker local time when the messages are appended."))))))),
            THROTTLE_TIME_MS);

    private static final Schema PRODUCE_RESPONSE_V3 = PRODUCE_RESPONSE_V2;

    /**
     * The body of PRODUCE_RESPONSE_V4 is the same as PRODUCE_RESPONSE_V3.
     * The version number is bumped up to indicate that the client supports KafkaStorageException.
     * The KafkaStorageException will be translated to NotLeaderForPartitionException in the response if version <= 3
     */
    private static final Schema PRODUCE_RESPONSE_V4 = PRODUCE_RESPONSE_V3;


    /**
     * Add in the log_start_offset field to the partition response to filter out spurious OutOfOrderSequencExceptions
     * on the client.
     */
    public static final Schema PRODUCE_RESPONSE_V5 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE,
                            new Field(BASE_OFFSET_KEY_NAME, INT64),
                            new Field(LOG_APPEND_TIME_KEY_NAME, INT64, "The timestamp returned by broker after appending " +
                                    "the messages. If CreateTime is used for the topic, the timestamp will be -1. " +
                                    "If LogAppendTime is used for the topic, the timestamp will be the broker local " +
                                    "time when the messages are appended."),
                            LOG_START_OFFSET_FIELD)))))),
            THROTTLE_TIME_MS);


    public static Schema[] schemaVersions() {
        return new Schema[]{PRODUCE_RESPONSE_V0, PRODUCE_RESPONSE_V1, PRODUCE_RESPONSE_V2, PRODUCE_RESPONSE_V3,
            PRODUCE_RESPONSE_V4, PRODUCE_RESPONSE_V5};
    }
    
    private Errors error;
    private long offset;
    private String topic;

	public ProduceResponse(Struct struct) {
		for (Object topicResponse : struct.getArray(RESPONSES_KEY_NAME)) {
			Struct topicRespStruct = (Struct) topicResponse;
			topic = topicRespStruct.get(TOPIC_NAME);
			for (Object partResponse : topicRespStruct.getArray(PARTITION_RESPONSES_KEY_NAME)) {
				Struct partRespStruct = (Struct) partResponse;
//				int partition = partRespStruct.get(PARTITION_ID);
				error = Errors.forCode(partRespStruct.get(ERROR_CODE));
				offset = partRespStruct.getLong(BASE_OFFSET_KEY_NAME);
//				long logAppendTime = partRespStruct.getLong(LOG_APPEND_TIME_KEY_NAME);
//				long logStartOffset = partRespStruct.getOrElse(LOG_START_OFFSET_FIELD, INVALID_OFFSET);
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
	
	
}
