package com.feeyo.kafka.codec;

import static com.feeyo.kafka.protocol.CommonFields.ERROR_CODE;
import static com.feeyo.kafka.protocol.CommonFields.PARTITION_ID;
import static com.feeyo.kafka.protocol.CommonFields.THROTTLE_TIME_MS;
import static com.feeyo.kafka.protocol.CommonFields.TOPIC_NAME;
import static com.feeyo.kafka.protocol.types.Type.INT64;
import static com.feeyo.kafka.protocol.types.Type.RECORDS;

import java.util.List;

import com.feeyo.kafka.protocol.types.ArrayOf;
import com.feeyo.kafka.protocol.types.Field;
import com.feeyo.kafka.protocol.types.Schema;
import com.feeyo.kafka.protocol.types.Struct;

/**
 * This wrapper supports all versions of the Fetch API
 */
public class FetchResponse {

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String PARTITIONS_KEY_NAME = "partition_responses";

    // partition level field names
    private static final String PARTITION_HEADER_KEY_NAME = "partition_header";
    private static final String HIGH_WATERMARK_KEY_NAME = "high_watermark";
    private static final String LAST_STABLE_OFFSET_KEY_NAME = "last_stable_offset";
    private static final String LOG_START_OFFSET_KEY_NAME = "log_start_offset";
    private static final String ABORTED_TRANSACTIONS_KEY_NAME = "aborted_transactions";
    private static final String RECORD_SET_KEY_NAME = "record_set";

    // aborted transaction field names
    private static final String PRODUCER_ID_KEY_NAME = "producer_id";
    private static final String FIRST_OFFSET_KEY_NAME = "first_offset";

    private static final Schema FETCH_RESPONSE_PARTITION_HEADER_V0 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(HIGH_WATERMARK_KEY_NAME, INT64, "Last committed offset."));
    private static final Schema FETCH_RESPONSE_PARTITION_V0 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V0),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    private static final Schema FETCH_RESPONSE_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V0)));

    private static final Schema FETCH_RESPONSE_V0 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));

    private static final Schema FETCH_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));
    // Even though fetch response v2 has the same protocol as v1, the record set in the response is different. In v1,
    // record set only includes messages of v0 (magic byte 0). In v2, record set can include messages of v0 and v1
    // (magic byte 0 and 1). For details, see Records, RecordBatch and Record.
    private static final Schema FETCH_RESPONSE_V2 = FETCH_RESPONSE_V1;

    // The partition ordering is now relevant - partitions will be processed in order they appear in request.
    private static final Schema FETCH_RESPONSE_V3 = FETCH_RESPONSE_V2;

    // The v4 Fetch Response adds features for transactional consumption (the aborted transaction list and the
    // last stable offset). It also exposes messages with magic v2 (along with older formats).
    private static final Schema FETCH_RESPONSE_ABORTED_TRANSACTION_V4 = new Schema(
            new Field(PRODUCER_ID_KEY_NAME, INT64, "The producer id associated with the aborted transactions"),
            new Field(FIRST_OFFSET_KEY_NAME, INT64, "The first offset in the aborted transaction"));

    private static final Schema FETCH_RESPONSE_ABORTED_TRANSACTION_V5 = FETCH_RESPONSE_ABORTED_TRANSACTION_V4;

    private static final Schema FETCH_RESPONSE_PARTITION_HEADER_V4 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(HIGH_WATERMARK_KEY_NAME, INT64, "Last committed offset."),
            new Field(LAST_STABLE_OFFSET_KEY_NAME, INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state " +
                    "of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
            new Field(ABORTED_TRANSACTIONS_KEY_NAME, ArrayOf.nullable(FETCH_RESPONSE_ABORTED_TRANSACTION_V4)));

    // FETCH_RESPONSE_PARTITION_HEADER_V5 added log_start_offset field - the earliest available offset of partition data that can be consumed.
    private static final Schema FETCH_RESPONSE_PARTITION_HEADER_V5 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(HIGH_WATERMARK_KEY_NAME, INT64, "Last committed offset."),
            new Field(LAST_STABLE_OFFSET_KEY_NAME, INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state " +
                    "of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
            new Field(LOG_START_OFFSET_KEY_NAME, INT64, "Earliest available offset."),
            new Field(ABORTED_TRANSACTIONS_KEY_NAME, ArrayOf.nullable(FETCH_RESPONSE_ABORTED_TRANSACTION_V5)));

    private static final Schema FETCH_RESPONSE_PARTITION_V4 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V4),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    private static final Schema FETCH_RESPONSE_PARTITION_V5 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V5),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    private static final Schema FETCH_RESPONSE_TOPIC_V4 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V4)));

    private static final Schema FETCH_RESPONSE_TOPIC_V5 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V5)));

    private static final Schema FETCH_RESPONSE_V4 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V4)));

    private static final Schema FETCH_RESPONSE_V5 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V5)));

    /**
     * The body of FETCH_RESPONSE_V6 is the same as FETCH_RESPONSE_V5.
     * The version number is bumped up to indicate that the client supports KafkaStorageException.
     * The KafkaStorageException will be translated to NotLeaderForPartitionException in the response if version <= 5
     */
    private static final Schema FETCH_RESPONSE_V6 = FETCH_RESPONSE_V5;

    // FETCH_REESPONSE_V7 added incremental fetch responses and a top-level error code.
    public static final Field.Int32 SESSION_ID = new Field.Int32("session_id", "The fetch session ID");

    private static final Schema FETCH_RESPONSE_V7 = new Schema(
        THROTTLE_TIME_MS,
        ERROR_CODE,
        SESSION_ID,
        new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V5)));

    public static Schema[] schemaVersions() {
        return new Schema[] {FETCH_RESPONSE_V0, FETCH_RESPONSE_V1, FETCH_RESPONSE_V2,
            FETCH_RESPONSE_V3, FETCH_RESPONSE_V4, FETCH_RESPONSE_V5, FETCH_RESPONSE_V6,
            FETCH_RESPONSE_V7};
    }


    public static final long INVALID_HIGHWATERMARK = -1L;
    public static final long INVALID_LAST_STABLE_OFFSET = -1L;
    public static final long INVALID_LOG_START_OFFSET = -1L;

    
    private String topic;
    private Errors responseErr;
    private Errors fetchErr;
    private int partition;
    private long lastStableOffset  = INVALID_LAST_STABLE_OFFSET;
    private long logStartOffset = INVALID_LOG_START_OFFSET;
    private List<Record> records;
    
    
	public FetchResponse(Struct struct) {

		for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
			Struct topicResponse = (Struct) topicResponseObj;
			topic = topicResponse.get(TOPIC_NAME);
			for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
				Struct partitionResponse = (Struct) partitionResponseObj;
				Struct partitionResponseHeader = partitionResponse.getStruct(PARTITION_HEADER_KEY_NAME);
				partition = partitionResponseHeader.get(PARTITION_ID);
				fetchErr = Errors.forCode(partitionResponseHeader.get(ERROR_CODE));
				if (partitionResponseHeader.hasField(LAST_STABLE_OFFSET_KEY_NAME))
					lastStableOffset = partitionResponseHeader.getLong(LAST_STABLE_OFFSET_KEY_NAME);
				if (partitionResponseHeader.hasField(LOG_START_OFFSET_KEY_NAME))
					logStartOffset = partitionResponseHeader.getLong(LOG_START_OFFSET_KEY_NAME);

				records = partitionResponse.getRecords(RECORD_SET_KEY_NAME);
			}
		}
		this.responseErr = Errors.forCode(struct.getOrElse(ERROR_CODE, (short) 0));
	}


	public String getTopic() {
		return topic;
	}

	public Errors getResponseErr() {
		return responseErr;
	}


	public Errors getFetchErr() {
		return fetchErr;
	}


	public int getPartition() {
		return partition;
	}


	public long getLastStableOffset() {
		return lastStableOffset;
	}


	public long getLogStartOffset() {
		return logStartOffset;
	}


	public List<Record> getRecords() {
		return records;
	}
	
	public boolean isCorrect() {
		return responseErr.getCode() == (short)0 && (fetchErr == null || fetchErr.getCode() == (short)0);
	}
	
	public String getErrorMessage() {
		if (responseErr.getCode() != (short)0) {
			return responseErr.getMessage();
		} else {
			return fetchErr.getMessage();
		}
	}
	
}
