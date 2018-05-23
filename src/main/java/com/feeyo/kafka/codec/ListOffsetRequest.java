package com.feeyo.kafka.codec;

import static com.feeyo.kafka.protocol.CommonFields.PARTITION_ID;
import static com.feeyo.kafka.protocol.CommonFields.TOPIC_NAME;
import static com.feeyo.kafka.protocol.types.Type.INT32;
import static com.feeyo.kafka.protocol.types.Type.INT64;
import static com.feeyo.kafka.protocol.types.Type.INT8;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.kafka.protocol.ApiKeys;
import com.feeyo.kafka.protocol.types.ArrayOf;
import com.feeyo.kafka.protocol.types.Field;
import com.feeyo.kafka.protocol.types.Schema;
import com.feeyo.kafka.protocol.types.Struct;

public class ListOffsetRequest {
	private static final String REPLICA_ID_KEY_NAME = "replica_id";
	private static final String ISOLATION_LEVEL_KEY_NAME = "isolation_level";
	private static final String TOPICS_KEY_NAME = "topics";

	// topic level field names
	private static final String PARTITIONS_KEY_NAME = "partitions";
	// partition level field names
	private static final String TIMESTAMP_KEY_NAME = "timestamp";
	private static final String MAX_NUM_OFFSETS_KEY_NAME = "max_num_offsets";

	private static final Schema LIST_OFFSET_REQUEST_PARTITION_V0 = new Schema(PARTITION_ID,
			new Field(TIMESTAMP_KEY_NAME, INT64, "Timestamp."),
			new Field(MAX_NUM_OFFSETS_KEY_NAME, INT32, "Maximum offsets to return."));
	private static final Schema LIST_OFFSET_REQUEST_PARTITION_V1 = new Schema(PARTITION_ID,
			new Field(TIMESTAMP_KEY_NAME, INT64, "The target timestamp for the partition."));

	private static final Schema LIST_OFFSET_REQUEST_TOPIC_V0 = new Schema(TOPIC_NAME, new Field(PARTITIONS_KEY_NAME,
			new ArrayOf(LIST_OFFSET_REQUEST_PARTITION_V0), "Partitions to list offset."));
	private static final Schema LIST_OFFSET_REQUEST_TOPIC_V1 = new Schema(TOPIC_NAME, new Field(PARTITIONS_KEY_NAME,
			new ArrayOf(LIST_OFFSET_REQUEST_PARTITION_V1), "Partitions to list offset."));

	private static final Schema LIST_OFFSET_REQUEST_V0 = new Schema(
			new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
			new Field(TOPICS_KEY_NAME, new ArrayOf(LIST_OFFSET_REQUEST_TOPIC_V0), "Topics to list offsets."));
	private static final Schema LIST_OFFSET_REQUEST_V1 = new Schema(
			new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
			new Field(TOPICS_KEY_NAME, new ArrayOf(LIST_OFFSET_REQUEST_TOPIC_V1), "Topics to list offsets."));

	private static final Schema LIST_OFFSET_REQUEST_V2 = new Schema(
			new Field(REPLICA_ID_KEY_NAME, INT32, "Broker id of the follower. For normal consumers, use -1."),
			new Field(ISOLATION_LEVEL_KEY_NAME, INT8, "This setting controls the visibility of transactional records. "
					+ "Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED "
					+ "(isolation_level = 1), non-transactional and COMMITTED transactional records are visible. "
					+ "To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current "
					+ "LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the "
					+ "result, which allows consumers to discard ABORTED transactional records"),
			new Field(TOPICS_KEY_NAME, new ArrayOf(LIST_OFFSET_REQUEST_TOPIC_V1), "Topics to list offsets."));;

	public static Schema[] schemaVersions() {
		return new Schema[] { LIST_OFFSET_REQUEST_V0, LIST_OFFSET_REQUEST_V1, LIST_OFFSET_REQUEST_V2 };
	}
	
	private final short version;
	private final String topic;
	private final int partition;
	private final long timestamp;
	private final int replicaId;
	private final byte isolationLevel;
	
	public ListOffsetRequest (short version, String topic, int partition, long timestamp, int replicaId, byte isolationLevel) {
		this.version = version;
		this.topic = topic;
		this.partition = partition;
		this.timestamp = timestamp;
		this.replicaId = replicaId;
		this.isolationLevel = isolationLevel;
	}
	
	public Struct toStruct() {
		Struct struct = new Struct(ApiKeys.LIST_OFFSETS.requestSchema(version));

		struct.set(REPLICA_ID_KEY_NAME, replicaId);

		if (struct.hasField(ISOLATION_LEVEL_KEY_NAME)) {
			struct.set(ISOLATION_LEVEL_KEY_NAME, isolationLevel);
		}
		List<Struct> topicArray = new ArrayList<>();
		
		Struct topicData = struct.instance(TOPICS_KEY_NAME);
		topicData.set(TOPIC_NAME, topic);
		
		List<Struct> partitionArray = new ArrayList<>();
		Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
		partitionData.set(PARTITION_ID, partition);
		partitionData.set(TIMESTAMP_KEY_NAME, timestamp);
		partitionArray.add(partitionData);

		topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
		topicArray.add(topicData);
		struct.set(TOPICS_KEY_NAME, topicArray.toArray());
		return struct;
	}

}
