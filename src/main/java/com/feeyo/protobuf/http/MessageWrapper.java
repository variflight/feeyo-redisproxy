package com.feeyo.protobuf.http;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.protobuf.codec.PBDecoder;
import com.feeyo.protobuf.codec.PBEncoder;
import com.feeyo.util.ByteUtil;
import com.google.common.primitives.Bytes;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.MessageLiteOrBuilder;

public class MessageWrapper {

	private final Logger LOGGER = LoggerFactory.getLogger(MessageWrapper.class);
	private final Charset charset = Charset.forName("UTF-8");

	private final byte[] MAGIC_CODE = new byte[] { 0x00, 0x00, 0x01, 0x0f };

	// ==== wrap-index ===== size ===========
	// totalSize 			4
	// nameSize 			4
	// className 			nameSize
	// content 				content.length
	// MAGIC_CODE 			MAGIC_CODE.length

	public byte[] wrapIn(MessageLiteOrBuilder msg) throws InvalidProtocolBufferException {

		String className = msg.getClass().getName();
		int nameSize = className.getBytes(charset).length;
		PBEncoder encoder = new PBEncoder();
		byte[] content = encoder.encode(msg);
		int totalSize = 4 + 4 + nameSize + content.length + MAGIC_CODE.length;

		if (totalSize == 0 || nameSize == 0 || className == null || content == null || content.length == 0)
			return null;
		
		byte[] ret = new byte[totalSize];

		System.arraycopy(ByteUtil.intToBytes(totalSize), 0, ret, 0, 4);
		System.arraycopy(ByteUtil.intToBytes(nameSize), 0, ret, 4, 4);
		System.arraycopy(className.getBytes(charset), 0, ret, 4 + 4, nameSize);
		System.arraycopy(content, 0, ret, 4 + 4 + nameSize, content.length);
		System.arraycopy(MAGIC_CODE, 0, ret, 4 + 4 + nameSize + content.length, MAGIC_CODE.length);

		return ret;
	}

	public MessageLite wrapOutSingle(byte[] buf) {

		if (!isLegal(buf))
			return null;

		try {
			int nameSize = ByteUtil.bytesToInt(buf[7], buf[6], buf[5], buf[4]);
			byte[] classNameBuf = new byte[nameSize];
			System.arraycopy(buf, 4 + 4, classNameBuf, 0, nameSize);

			String className = new String(classNameBuf, charset);
			Class<?> clazz = Class.forName(className);
			PBDecoder decoder = new PBDecoder(clazz);
			
			int contentLength = buf.length - 4 - 4 - nameSize - MAGIC_CODE.length;
			byte[] content = new byte[contentLength];
			System.arraycopy(buf, 4 + 4 + nameSize, content, 0, contentLength);
			
			return decoder.decode(content);
			
		} catch (NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException
				| ClassNotFoundException | InstantiationException | IllegalAccessException
				| InvalidProtocolBufferException e) {
			LOGGER.error("wrap out message exception {}", e.getMessage());
		}
		return null;

	}

	public byte[] wrapIn(List<MessageLiteOrBuilder> msgList) throws InvalidProtocolBufferException {

		byte[] ret = new byte[0];

		for (MessageLiteOrBuilder msg : msgList) {
			byte[] buf = wrapIn(msg);
			ret = ByteUtil.byteMerge(buf, ret);
		}
		return ret;
	}

	public List<MessageLiteOrBuilder> wrapOut(byte[] buf) {

		int ptr = 0;
		List<MessageLiteOrBuilder> msgList = new ArrayList<MessageLiteOrBuilder>();

		while (ptr < buf.length) {

			int totalSize = ByteUtil.bytesToInt(buf[ptr + 3], buf[ptr + 1], buf[ptr + 1], buf[ptr + 0]);
			byte[] protoBuf = new byte[totalSize];
			System.arraycopy(buf, ptr, protoBuf, 0, totalSize);
			
			MessageLite msg = wrapOutSingle(protoBuf);
			if(msg != null) {
				msgList.add(msg);
			}else {
				LOGGER.error("parse msg failed");
			}

			ptr += totalSize;
		}

		return msgList;
	}

	private boolean isLegal(byte[] buf) {
		return buf != null && buf.length > 12 && buf.length == ByteUtil.bytesToInt(buf[3], buf[2], buf[1], buf[0])
				&& Bytes.indexOf(buf, MAGIC_CODE) == buf.length - MAGIC_CODE.length;
	}

}
