package com.feeyo.kafka.protocol.types;

/**
 * A field definition bound to a particular schema.
 * 
 * @see https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/protocol/types/BoundField.java
 */
public class BoundField {
    public final Field def;
    final int index;
    final Schema schema;

    public BoundField(Field def, Schema schema, int index) {
        this.def = def;
        this.schema = schema;
        this.index = index;
    }

    @Override
    public String toString() {
        return def.name + ":" + def.type;
    }
}
