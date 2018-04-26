package com.feeyo.kafka.protocol.types;

/**
 * A field definition bound to a particular schema.
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
