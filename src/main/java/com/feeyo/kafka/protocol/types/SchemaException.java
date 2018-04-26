package com.feeyo.kafka.protocol.types;

/**
 *  Thrown if the protocol schema validation fails while parsing request or response.
 */
public class SchemaException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SchemaException(String message) {
        super(message);
    }

}
