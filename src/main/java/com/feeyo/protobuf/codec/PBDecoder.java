package com.feeyo.protobuf.codec;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.MessageLite.Builder;

//TODO hasParse + extensionRegistry
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
	
	public <T> PBDecoder(Class<T> clazz) throws InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
	    this(clazz, null);
	}

    public <T> PBDecoder(Class<T> clazz, ExtensionRegistry extensionRegistry) throws InstantiationException, IllegalAccessException,  NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
        this(clazz, (ExtensionRegistryLite) extensionRegistry);
    }

    public <T> PBDecoder(Class<T> clazz, ExtensionRegistryLite extensionRegistry) throws InstantiationException, IllegalAccessException,  NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
        
    	if (clazz == null) {
            throw new NullPointerException("prototype");
        }
    	
    	Method method = clazz.getDeclaredMethod("newBuilder");
    	Builder builder = (Builder) method.invoke(clazz);
    	MessageLite proto = builder.build();
    	
        this.prototype = proto.getDefaultInstanceForType();
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
               return prototype.getParserForType().parseFrom(
                        protobuf, extensionRegistry);
            } else {
               return prototype.newBuilderForType().mergeFrom(
                        protobuf, extensionRegistry).build();
            }
        }
	}
	
}
