package com.feeyo.protobuf.codec;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.util.ByteUtil;
import com.google.common.primitives.Bytes;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class PBDecoderV2 {
	
	private final Logger LOGGER = LoggerFactory.getLogger(PBDecoderV2.class);
	private final Charset charset = Charset.forName("UTF-8");

	private final byte[] MAGIC_CODE = new byte[] { (byte) 0x7f, (byte) 0xff };

	// ==== wrap-index ===== size ===========
	// totalSize 			4
	// nameSize 			4
	// className 			nameSize
	// content 				content.length
	// MAGIC_CODE 			MAGIC_CODE.length
	
	public MessageLite decodeOneMsg(byte[] buf) {

		if (!isLegal(buf))
			return null;
		try {
			int nameSize = ByteUtil.bytesToInt(buf[7], buf[6], buf[5], buf[4]);
			byte[] classNameBuf = new byte[nameSize];
			System.arraycopy(buf, 4 + 4, classNameBuf, 0, nameSize);

			String className = new String(classNameBuf, charset);
			Class<?> clazz = Class.forName(className);
			Method method = clazz.getDeclaredMethod("getDefaultInstance");
			MessageLite proto = (MessageLite) method.invoke(clazz);
			PBDecoder decoder = new PBDecoder(proto);
			
			int contentLength = buf.length - 4 - 4 - nameSize - MAGIC_CODE.length;
			byte[] content = new byte[contentLength];
			System.arraycopy(buf, 4 + 4 + nameSize, content, 0, contentLength);
			
			return decoder.decode(content);
			} catch (ClassNotFoundException | NoSuchMethodException | SecurityException 
					| IllegalAccessException | IllegalArgumentException | InvocationTargetException | InvalidProtocolBufferException e) {
				LOGGER.error("wrap out message exception {}", e.getMessage());
			}
		return null;

	}
	
	public List<MessageLite> decode(byte[] buf) {

		int ptr = 0;
		List<MessageLite> msgList = new ArrayList<MessageLite>();

		while (ptr < buf.length) {

			int totalSize = ByteUtil.bytesToInt(buf[ptr + 3], buf[ptr + 1], buf[ptr + 1], buf[ptr + 0]);
			byte[] protoBuf = new byte[totalSize];
			System.arraycopy(buf, ptr, protoBuf, 0, totalSize);
			
			MessageLite msg = decodeOneMsg(protoBuf);
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
