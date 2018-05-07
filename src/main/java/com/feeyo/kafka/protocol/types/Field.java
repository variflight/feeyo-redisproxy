package com.feeyo.kafka.protocol.types;

/**
 * 
 * @see https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/protocol/types/Field.java
 *
 */
public class Field {
    public final String name;
    public final String docString;
    public final Type type;
    public final boolean hasDefaultValue;
    public final Object defaultValue;

    public Field(String name, Type type, String docString, boolean hasDefaultValue, Object defaultValue) {
        this.name = name;
        this.docString = docString;
        this.type = type;
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;

        if (hasDefaultValue)
            type.validate(defaultValue);
    }

    public Field(String name, Type type, String docString) {
        this(name, type, docString, false, null);
    }

    public Field(String name, Type type, String docString, Object defaultValue) {
        this(name, type, docString, true, defaultValue);
    }

    public Field(String name, Type type) {
        this(name, type, null, false, null);
    }

    public static class Int8 extends Field {
        public Int8(String name, String docString) {
            super(name, Type.INT8, docString, false, null);
        }
    }

    public static class Int32 extends Field {
        public Int32(String name, String docString) {
            super(name, Type.INT32, docString, false, null);
        }

        public Int32(String name, String docString, int defaultValue) {
            super(name, Type.INT32, docString, true, defaultValue);
        }
    }

    public static class Int64 extends Field {
        public Int64(String name, String docString) {
            super(name, Type.INT64, docString, false, null);
        }

        public Int64(String name, String docString, long defaultValue) {
            super(name, Type.INT64, docString, true, defaultValue);
        }
    }

    public static class Int16 extends Field {
        public Int16(String name, String docString) {
            super(name, Type.INT16, docString, false, null);
        }
    }

    public static class Str extends Field {
        public Str(String name, String docString) {
            super(name, Type.STRING, docString, false, null);
        }
    }

    public static class NullableStr extends Field {
        public NullableStr(String name, String docString) {
            super(name, Type.NULLABLE_STRING, docString, false, null);
        }
    }
}
