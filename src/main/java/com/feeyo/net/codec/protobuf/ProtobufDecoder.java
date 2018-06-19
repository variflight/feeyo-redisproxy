package com.feeyo.net.codec.protobuf;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.Decoder;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class ProtobufDecoder implements Decoder<List<MessageLite>> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufDecoder.class);
	
	private static final byte[] MAGIC_CODE = new byte[] { (byte) 0x7f, (byte) 0xff };

	private final MessageLite prototype;
	private final ExtensionRegistryLite extensionRegistry;
	
	private static boolean HAS_PARSER = false;

	private byte[] _buffer = null;
	private int _offset = 0;

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

	public ProtobufDecoder(MessageLite prototype) {
		this(prototype, null);
	}

	public ProtobufDecoder(MessageLite prototype, ExtensionRegistry extensionRegistry) {
		this(prototype, (ExtensionRegistryLite) extensionRegistry);
	}

	public ProtobufDecoder(MessageLite prototype, ExtensionRegistryLite extensionRegistry) {

		if (prototype == null) {
			throw new NullPointerException("prototype");
		}

		this.prototype = prototype.getDefaultInstanceForType();
		this.extensionRegistry = extensionRegistry;
	}	

	@Override
	public List<MessageLite> decode(byte[] buf) {
		
		if (buf == null )
			return null;
		
		append(buf);
		
		if(_buffer.length < 4)
			return null;
		
		List<MessageLite> list = null;
		try {
			while( _offset != _buffer.length ) {
				
				int totalSize = 
						_buffer[_offset + 3] & 0xFF 
						| (_buffer[_offset + 2] & 0xFF) << 8
						| (_buffer[_offset + 1] & 0xFF) << 16 
						| (_buffer[_offset] & 0xFF) << 24;
				
				if ( _buffer.length >= _offset + totalSize ) {
					
					int contentLen = totalSize - 4 - MAGIC_CODE.length;
					byte[] content = new byte[contentLen];
					
					System.arraycopy(_buffer, _offset + 4, content, 0, contentLen);
	
					MessageLite msg = decodeMsg(content);
					
					if(list == null)
						list = new ArrayList<MessageLite>();
					
					list.add(msg);
					_offset += totalSize;
					
				}else {
					// data not enough
					throw new IndexOutOfBoundsException("No enough data.");
				}
				
			}
		
		} catch (InvalidProtocolBufferException e) {
			LOGGER.error(e.getMessage());
			return null;
		}
		
		reset();
		
		return list;
	}
	
	public void reset() {
		_buffer = null;
		_offset = 0;
	}
	
	private void append(byte[] newBuffer) {
		if (newBuffer == null) {
			return;
		}

		if (_buffer == null) {
			_buffer = newBuffer;
			return;
		}

		_buffer = margeByteArray(_buffer, newBuffer);
	}
	
	private byte[] margeByteArray(byte[] a, byte[] b) {
		byte[] result = new byte[a.length + b.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}
	
	private MessageLite decodeMsg(byte[] protobuf) throws InvalidProtocolBufferException {

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
