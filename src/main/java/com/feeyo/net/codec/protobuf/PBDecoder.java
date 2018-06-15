package com.feeyo.net.codec.protobuf;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.util.ByteUtil;
import com.google.common.primitives.Bytes;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class PBDecoder {
	
	private final Logger LOGGER = LoggerFactory.getLogger(PBDecoder.class);

	private final byte[] MAGIC_CODE = new byte[] { (byte) 0x7f, (byte) 0xff };

	private final MessageLite prototype;
	private final ExtensionRegistryLite extensionRegistry;
	private static boolean HAS_PARSER = false;

	private boolean hasAllSize = false;
	
	private byte[] msgBuffCache = new byte[0];
	private int ptr = 0;
	private int allSize = -1;

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

	public PBDecoder(MessageLite prototype, boolean hasAllSize) {
		this(prototype, null, hasAllSize);
	}

	public PBDecoder(MessageLite prototype, ExtensionRegistry extensionRegistry, boolean hasAllSize) {
		this(prototype, (ExtensionRegistryLite) extensionRegistry, hasAllSize);
	}

	public PBDecoder(MessageLite prototype, ExtensionRegistryLite extensionRegistry, boolean hasAllSize) {

		if (prototype == null) {
			throw new NullPointerException("prototype");
		}

		this.prototype = prototype.getDefaultInstanceForType();
		this.extensionRegistry = extensionRegistry;
		this.hasAllSize = hasAllSize;
	}
	
	public List<MessageLite> decode(byte[] buf) throws InvalidProtocolBufferException {
		
		if(buf == null)
			return null;
		
		if(hasAllSize) {
			
			if(allSize == -1) {
				msgBuffCache = ByteUtil.byteMerge(msgBuffCache, buf);
				if(msgBuffCache.length < 4)
					return null;
				
				allSize = ByteUtil.bytesToInt(msgBuffCache[3], msgBuffCache[2], msgBuffCache[1], msgBuffCache[0]);
				
				msgBuffCache = new byte[allSize];
				int read = Math.min(allSize - ptr, buf.length - 4);
				System.arraycopy(buf, 4, msgBuffCache, ptr, read);
				ptr += read;
				if(ptr < allSize)		//数据不完整
					return null;
			}
			
			if(ptr < allSize) {
				 int read = Math.min(allSize - ptr, buf.length);
				 System.arraycopy(buf, 0, msgBuffCache, ptr, read);
				 ptr += read;
				 if(ptr < allSize)		//数据不完整
					 return null;	
			}
			
			return decodeAllMsg(msgBuffCache);
		}
		
		return decodeAllMsg(buf);
	}
	
	private List<MessageLite> decodeAllMsg(byte[] buf) throws InvalidProtocolBufferException{
		
		List<MessageLite> msgList = new ArrayList<MessageLite>();
		int pos = 0;
		while (pos < buf.length) {

			int singleSize = ByteUtil.bytesToInt(buf[pos + 3], buf[pos + 1], buf[pos + 1], buf[pos + 0]);
			byte[] protoBuf = new byte[singleSize];
			System.arraycopy(buf, pos, protoBuf, 0, singleSize);

			MessageLite msg = decodeWrapMsg(protoBuf);
			if (msg != null) {
				msgList.add(msg);
			} else {
				LOGGER.error("parse msg failed");
			}

			pos += singleSize;
		}
		
		if(hasAllSize) {
			reset();
		}

		return msgList;
	}
	
	// ==== wrap-index ===== size ===========
	// singleSize 			4
	// content 				content.length
	// MAGIC_CODE 			MAGIC_CODE.length

	private MessageLite decodeWrapMsg(byte[] buf) throws InvalidProtocolBufferException {

		if (!isLegal(buf))
		return null;
		int contentLength = buf.length - 4 - MAGIC_CODE.length;
		byte[] content = new byte[contentLength];
		System.arraycopy(buf, 4, content, 0, contentLength);

		return decodeOriginMsg(content);
	}

	private MessageLite decodeOriginMsg(byte[] protobuf) throws InvalidProtocolBufferException {

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

	private void reset() {
		msgBuffCache = new byte[0];
		ptr = 0;
		allSize = -1;
	}
	
	private boolean isLegal(byte[] buf) {
		return buf != null && buf.length > 4 + MAGIC_CODE.length && buf.length == ByteUtil.bytesToInt(buf[3], buf[2], buf[1], buf[0])
				&& Bytes.indexOf(buf, MAGIC_CODE) == buf.length - MAGIC_CODE.length;
	}

}
