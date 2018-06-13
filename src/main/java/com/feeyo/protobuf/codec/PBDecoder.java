package com.feeyo.protobuf.codec;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class PBDecoder implements Decoder {

	private final MessageLite prototype;
	private final ExtensionRegistryLite extensionRegistry;
	private static boolean HAS_PARSER = false;

	static {
		boolean hasParser = false;
		try {
			// MessageLite.getParserForType() is not available until protobuf 2.5.0.
			MessageLite.class.getDeclaredMethod("getParserForType");
			hasParser = true;
		} catch (Throwable t) {
			// Ignore
		}

		HAS_PARSER = hasParser;
	}

	public PBDecoder(MessageLite prototype) {
		this(prototype, null);
	}

	public PBDecoder(MessageLite prototype, ExtensionRegistry extensionRegistry) {
		this(prototype, (ExtensionRegistryLite) extensionRegistry);
	}

	public PBDecoder(MessageLite prototype, ExtensionRegistryLite extensionRegistry) {
		
		if (prototype == null) {
			throw new NullPointerException("prototype");
		}
		this.prototype = prototype.getDefaultInstanceForType();
		this.extensionRegistry = extensionRegistry;
	}

	public MessageLite decode(byte[] protobuf) throws InvalidProtocolBufferException {

		if (extensionRegistry == null) {
			if (HAS_PARSER) {
				return prototype.getParserForType().parseFrom(protobuf);
			} else {
				return prototype.newBuilderForType().mergeFrom(protobuf).build();
			}
		} else {
			if (HAS_PARSER) {
				return prototype.getParserForType().parseFrom(protobuf, extensionRegistry);
			} else {
				return prototype.newBuilderForType().mergeFrom(protobuf, extensionRegistry).build();
			}
		}
	}

}
