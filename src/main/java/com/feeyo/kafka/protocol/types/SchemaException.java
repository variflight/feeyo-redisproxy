package com.feeyo.kafka.protocol.types;

/**
 *  Thrown if the protocol schema validation fails while parsing request or response.
 *  
 *  @see https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/protocol/types/SchemaException.java
 */
public class SchemaException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SchemaException(String message) {
        super(message);
    }

}
