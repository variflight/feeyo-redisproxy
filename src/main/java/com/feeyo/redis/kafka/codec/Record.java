package com.feeyo.redis.kafka.codec;

import java.nio.ByteBuffer;

import com.feeyo.redis.kafka.util.ByteUtils;
import com.feeyo.redis.kafka.util.Crc32C;

public class Record {
	private static final int NULL_VARINT_SIZE_BYTES = ByteUtils.sizeOfVarint(-1);
	
    static final int BASE_OFFSET_OFFSET = 0;
    static final int BASE_OFFSET_LENGTH = 8;
    static final int LENGTH_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
    static final int LENGTH_LENGTH = 4;
    static final int PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    static final int PARTITION_LEADER_EPOCH_LENGTH = 4;
    static final int MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
    static final int MAGIC_LENGTH = 1;
    static final int CRC_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    static final int CRC_LENGTH = 4;
    static final int ATTRIBUTES_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int ATTRIBUTE_LENGTH = 2;
    public static final int LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    static final int LAST_OFFSET_DELTA_LENGTH = 4;
    static final int FIRST_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
    static final int FIRST_TIMESTAMP_LENGTH = 8;
    static final int MAX_TIMESTAMP_OFFSET = FIRST_TIMESTAMP_OFFSET + FIRST_TIMESTAMP_LENGTH;
    static final int MAX_TIMESTAMP_LENGTH = 8;
    static final int PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
    static final int PRODUCER_ID_LENGTH = 8;
    static final int PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
    static final int PRODUCER_EPOCH_LENGTH = 2;
    static final int BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
    static final int BASE_SEQUENCE_LENGTH = 4;
    public static final int RECORDS_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
    static final int RECORDS_COUNT_LENGTH = 4;
    static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
    public static final int RECORD_BATCH_OVERHEAD = RECORDS_OFFSET;
    
    private static final byte COMPRESSION_CODEC_MASK = 0x07;
    private static final byte TRANSACTIONAL_FLAG_MASK = 0x10;
    private static final int CONTROL_FLAG_MASK = 0x20;
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;
    
    static int OFFSET_OFFSET = 0;
    static int OFFSET_LENGTH = 8;
    static int SIZE_OFFSET = OFFSET_OFFSET + OFFSET_LENGTH;
    static int SIZE_LENGTH = 4;
    static int LOG_OVERHEAD = SIZE_OFFSET + SIZE_LENGTH;
	
	private long offset;
	private int offsetDelta;
	private long timestamp;
	private long timestampDelta;
	private byte[] key;
	private byte[] value;
	private String topic;
	private byte magic;
	
	public Record() {
		this(0, null, null, null);
	}
	
	public Record(long offset, String topic, byte[] key, byte[] value) {
		this.offset = offset;
		this.topic = topic;
		this.key = key;
		this.value = value;
	}
	
	public Record(ByteBuffer buffer) {
//		public String toString() {
//	        return "RecordBatch(magic=" + magic() + ", offsets=[" + baseOffset() + ", " + lastOffset() + "], " +
//	                "compression=" + compressionType() + ", timestampType=" + timestampType() + ", crc=" + checksum() + ")";
//	    }

		if (buffer.limit() == buffer.position()) {
			return;
		}
		//		return DefaultRecord.readFrom(buffer, baseOffset, firstTimestamp, baseSequence, logAppendTime);
		this.offset = buffer.getLong(BASE_OFFSET_OFFSET);
//		int x = LOG_OVERHEAD + buffer.getInt(LENGTH_OFFSET);
//		int partitionLeaderEpoch = buffer.getInt(PARTITION_LEADER_EPOCH_OFFSET);
//		this.magic = buffer.get(MAGIC_OFFSET);
//		long checkSum = ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
//		long firstTimestamp = buffer.getLong(FIRST_TIMESTAMP_OFFSET);
//		long baseSequence = buffer.getInt(BASE_SEQUENCE_OFFSET);
//		byte attru = (byte)buffer.getShort(ATTRIBUTES_OFFSET);
		
		buffer.position(RECORDS_OFFSET);
		
		int sizeOfBodyInBytes = ByteUtils.readVarint(buffer);
        if (buffer.remaining() < sizeOfBodyInBytes)
            return ;
		
		int recordStart = buffer.position();
        byte attributes = buffer.get();
        long timestampDelta = ByteUtils.readVarlong(buffer);

        int offsetDelta = ByteUtils.readVarint(buffer);
        long offset = this.offset + offsetDelta;

        int keySize = ByteUtils.readVarint(buffer);
        if (keySize >= 0) {
            key = new byte[keySize];
			for (int i = 0; i < keySize; i++) {
				key[i] = buffer.get(buffer.position() + i);
			}
			
            buffer.position(buffer.position() + keySize);
        }

        int valueSize = ByteUtils.readVarint(buffer);
        if (valueSize >= 0) {
            value = new byte[valueSize];
			for (int i = 0; i < valueSize; i++) {
				value[i] = buffer.get(buffer.position() + i);
			}
            buffer.position(buffer.position() + valueSize);
        }

        int numHeaders = ByteUtils.readVarint(buffer);
//        if (numHeaders < 0)
//            throw new InvalidRecordException("Found invalid number of record headers " + numHeaders);
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}
	
	public long getTimestampDelta() {
		return timestampDelta;
	}

	public void setTimestampDelta(long timestampDelta) {
		this.timestampDelta = timestampDelta;
	}

	public int sizeInBytes() {
		int bodySize = recordBodySizeInBytes();
		return ByteUtils.sizeOfVarint(bodySize) + bodySize + RECORD_BATCH_OVERHEAD;
	}
	
	public int getOffsetDelta() {
		return offsetDelta;
	}

	public void setOffsetDelta(int offsetDelta) {
		this.offsetDelta = offsetDelta;
	}

	private int recordBodySizeInBytes() {
		int keySize = key == null ? -1 : key.length;
        int valueSize = value == null ? -1 : value.length;

        int size = 1; // always one byte for attributes and 
		size += ByteUtils.sizeOfVarint(offsetDelta);
        size += ByteUtils.sizeOfVarlong(timestampDelta);
        size += sizeOf(keySize, valueSize);
		return size;
	}
	
	private int sizeOf(int keySize, int valueSize) {
		int size = 0;
		if (keySize < 0)
			size += NULL_VARINT_SIZE_BYTES;
		else
			size += ByteUtils.sizeOfVarint(keySize) + keySize;

		if (valueSize < 0)
			size += NULL_VARINT_SIZE_BYTES;
		else
			size += ByteUtils.sizeOfVarint(valueSize) + valueSize;

		// TODO 还有个header，没确定是什么 length设置成0
		size += ByteUtils.sizeOfVarint(0);
		return size;
	}
	
	public ByteBuffer get() {
		int size = sizeInBytes();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		
		buffer.position(RECORD_BATCH_OVERHEAD);
		
		ByteUtils.writeVarint(recordBodySizeInBytes(), buffer);
		byte attributes = 0; // there are no used record attributes at the moment
		buffer.put(attributes);
		// timestampDelta
		ByteUtils.writeVarlong(timestampDelta, buffer);
		// offsetDelta
		ByteUtils.writeVarint(offsetDelta, buffer);
		if (key == null) {
			ByteUtils.writeVarint(-1, buffer);
		} else {
			int keySize = key.length;
			ByteUtils.writeVarint(keySize, buffer);
			buffer.put(key);
		}
		if (value == null) {
			ByteUtils.writeVarint(-1, buffer);
		} else {
			int valueSize = value.length;
			ByteUtils.writeVarint(valueSize, buffer);
			buffer.put(value);
		}
		ByteUtils.writeVarint(0, buffer);
		
		buffer.position(0);
		writeHeader(size, buffer);
		buffer.position(size);
		buffer.rewind();
		return buffer;
	}

	private void writeHeader(int size, ByteBuffer buffer) {
		long baseOffset = 0;
		int lastOffsetDelta = 0;
		short attributes = computeAttributes(false);
		int partitionLeaderEpoch = -1;
		byte magic = (byte) 2;
		long producerId = -1;
		short epoch = (short) -1;
		int sequence = -1;
		int numRecords = 1;
		
		buffer.putLong(BASE_OFFSET_OFFSET, baseOffset);
		buffer.putInt(LENGTH_OFFSET, size - LOG_OVERHEAD);
		buffer.putInt(PARTITION_LEADER_EPOCH_OFFSET, partitionLeaderEpoch);
		buffer.put(MAGIC_OFFSET, magic);
		buffer.putShort(ATTRIBUTES_OFFSET, attributes);
		buffer.putLong(FIRST_TIMESTAMP_OFFSET, timestamp);
		buffer.putLong(MAX_TIMESTAMP_OFFSET, timestamp);
		buffer.putInt(LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta);
		buffer.putLong(PRODUCER_ID_OFFSET, producerId);
		buffer.putShort(PRODUCER_EPOCH_OFFSET, epoch);
		buffer.putInt(BASE_SEQUENCE_OFFSET, sequence);
		buffer.putInt(RECORDS_COUNT_OFFSET, numRecords);
		long crc = Crc32C.compute(buffer, ATTRIBUTES_OFFSET, size - ATTRIBUTES_OFFSET);
		buffer.putInt(CRC_OFFSET, (int) crc);
		buffer.position(RECORD_BATCH_OVERHEAD);
	}
	
	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	private byte computeAttributes(boolean isTransactional) {
		return  isTransactional ? TRANSACTIONAL_FLAG_MASK : 0;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("offset=").append(offset).append(",").append("timestamp=").append(timestamp).append(",")
				.append("key=").append(key).append(",").append("value=").append(value).append(",");
		return sb.toString();
		
//		 return String.format("DefaultRecord(offset=%d, timestamp=%d, key=%d bytes, value=%d bytes)",
//	                offset,
//	                timestamp,
//	                key == null ? 0 : key.limit(),
//	                value == null ? 0 : value.limit());
	}
	
}
