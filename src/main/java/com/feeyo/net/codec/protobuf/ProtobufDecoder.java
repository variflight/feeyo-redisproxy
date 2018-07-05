package com.feeyo.net.codec.protobuf;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.Decoder;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * 
 * @author xuwenfeng
 *
 */
public class ProtobufDecoder<T extends MessageLite> implements Decoder<List<T>> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufDecoder.class);
	
	//
	private boolean isCustomPkg = false;

	private final MessageLite prototype;
	private final ExtensionRegistry extensionRegistry;

	private static boolean HAS_PARSER = false;

	private byte[] _buffer = null;
	private int _offset = 0;

	static {
		boolean hasParser = false;
		try {
			// MessageLite.getParserForType() is not available until protobuf
			// 2.5.0.
			MessageLite.class.getDeclaredMethod("getParserForType");
			hasParser = true;
		} catch (Throwable t) {
			// Ignore
		}

		HAS_PARSER = hasParser;
	}

	public ProtobufDecoder(T prototype, boolean isCustomPkg) {
		this(prototype, null, isCustomPkg);
	}

	public ProtobufDecoder(T prototype, ExtensionRegistry extensionRegistry, boolean isCustomPkg) {

		if (prototype == null) {
			throw new NullPointerException("prototype");
		}

		this.prototype = prototype.getDefaultInstanceForType();
		this.extensionRegistry = extensionRegistry;
		
		//
		this.isCustomPkg = isCustomPkg;
	}
	
	public boolean isCustomPkg() {
		return isCustomPkg;
	}

	@Override
	public List<T> decode(byte[] buf) {

		if (buf == null)
			return null;

		append(buf);

		if (_buffer.length < 4)
			return null;

		
		
		List<T> list = null;
		try {
			
			if ( !isCustomPkg ) {
				//
				T msg = parse( _buffer );

				list = new ArrayList<T>();
				list.add(msg);
				
			}  else  {
			
				//
				while (_offset != _buffer.length) {
	
					//
					int totalSize = _buffer[_offset + 3] & 0xFF | (_buffer[_offset + 2] & 0xFF) << 8
							| (_buffer[_offset + 1] & 0xFF) << 16 | (_buffer[_offset] & 0xFF) << 24;
	
					if (_buffer.length >= _offset + totalSize) {
	
						//
						byte[] cb = new byte[totalSize - 4];
						System.arraycopy(_buffer, _offset + 4, cb, 0, cb.length);
	
						T msg = parse( cb );
						
						if (list == null)
							list = new ArrayList<T>();
						
						list.add(msg);
						
						_offset += totalSize;
	
					} else {
						// data not enough
						throw new IndexOutOfBoundsException("No enough data.");
					}
	
				}
			}

		} catch (InvalidProtocolBufferException e) {
			LOGGER.error("protobuf decode err:", e);
		}

		_buffer = null;
		_offset = 0;
		
		return list;
	}

	@SuppressWarnings("unchecked")
	private T parse(byte[] buf) throws InvalidProtocolBufferException {
		
		T msg = null;

		if (extensionRegistry == null) {
			if (HAS_PARSER) {
				msg = (T) prototype.getParserForType().parseFrom(buf);
			} else {
				msg = (T) prototype.newBuilderForType().mergeFrom(buf).build();
			}
		} else {
			if (HAS_PARSER) {
				msg = (T) prototype.getParserForType().parseFrom(buf, extensionRegistry);
			} else {
				msg = (T) prototype.newBuilderForType().mergeFrom(buf, extensionRegistry).build();
			}
		}
		return msg;
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

}
