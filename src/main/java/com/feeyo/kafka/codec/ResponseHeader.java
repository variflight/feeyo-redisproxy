package com.feeyo.kafka.codec;

import static com.feeyo.kafka.protocol.types.Type.INT32;

import java.nio.ByteBuffer;

import com.feeyo.kafka.protocol.types.BoundField;
import com.feeyo.kafka.protocol.types.Field;
import com.feeyo.kafka.protocol.types.Schema;
import com.feeyo.kafka.protocol.types.Struct;

/**
 * A response header in the kafka protocol.
 */
public class ResponseHeader {
    public static final Schema SCHEMA = new Schema(
            new Field("correlation_id", INT32, "The user-supplied value passed in with the request"));
    private static final BoundField CORRELATION_KEY_FIELD = SCHEMA.get("correlation_id");

    private final int correlationId;

    public ResponseHeader(Struct struct) {
        correlationId = struct.getInt(CORRELATION_KEY_FIELD);
    }

    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    public int sizeOf() {
        return toStruct().sizeOf();
    }

    public Struct toStruct() {
        Struct struct = new Struct(SCHEMA);
        struct.set(CORRELATION_KEY_FIELD, correlationId);
        return struct;
    }

    public int correlationId() {
        return correlationId;
    }

    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader(SCHEMA.read(buffer));
    }

}
