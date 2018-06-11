package com.feeyo.protobuf.codec;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

//TODO hasParse + extensionRegistry
public class ProtoBufDecoder {
	
	private final MessageLite  prototype;
	private final ExtensionRegistryLite extensionRegistry;
	private boolean hasParser = false;
	
	public ProtoBufDecoder(MessageLite prototype) {
	    this(prototype, null);
	}

    public ProtoBufDecoder(MessageLite prototype, ExtensionRegistry extensionRegistry) {
        this(prototype, (ExtensionRegistryLite) extensionRegistry);
    }

    public ProtoBufDecoder(MessageLite prototype, ExtensionRegistryLite extensionRegistry) {
        if (prototype == null) {
            throw new NullPointerException("prototype");
        }
        this.prototype = prototype.getDefaultInstanceForType();
        this.extensionRegistry = extensionRegistry;
    }
	
	public MessageLite decode(byte[] protobuf) throws InvalidProtocolBufferException {
		
		if (extensionRegistry == null) {
            if (hasParser) {
                return prototype.getParserForType().parseFrom(protobuf);
            } else {
               return prototype.newBuilderForType().mergeFrom(protobuf).build();
            }
        } else {
            if (hasParser) {
               return prototype.getParserForType().parseFrom(
                        protobuf, extensionRegistry);
            } else {
               return prototype.newBuilderForType().mergeFrom(
                        protobuf, extensionRegistry).build();
            }
        }
	}
	
}
