package com.feeyo.protobuf.codec;

import java.nio.charset.Charset;
import java.util.List;

import com.feeyo.util.ByteUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

public class PBEncoderV2 {
	
	private final Charset charset = Charset.forName("UTF-8");

	private final byte[] MAGIC_CODE = new byte[] { (byte) 0x7f, (byte) 0xff };

	// ==== wrap-index ===== size ===========
	// totalSize 			4
	// nameSize 			4
	// className 			nameSize
	// content 				content.length
	// MAGIC_CODE 			MAGIC_CODE.length

	public byte[] encode(MessageLite msg) throws InvalidProtocolBufferException {

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
	
	public byte[] encode(List<MessageLite> msgList) throws InvalidProtocolBufferException {

		byte[] ret = new byte[0];

		for (MessageLite msg : msgList) {
			byte[] buf = encode(msg);
			ret = ByteUtil.byteMerge(buf, ret);
		}
		return ret;
	}
}
