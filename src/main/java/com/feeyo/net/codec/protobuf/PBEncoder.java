package com.feeyo.net.codec.protobuf;

import java.util.List;

import com.feeyo.util.ByteUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

//
public class PBEncoder {
	
	private boolean isTcp = false;
	private final byte[] MAGIC_CODE = new byte[] { (byte) 0x7f, (byte) 0xff };
	
	public PBEncoder(boolean isTcp) {
		this.isTcp = isTcp;
	}
	
	
	private <T> byte[] encodeOrigin(T msg) throws InvalidProtocolBufferException {
		
		if (msg instanceof MessageLite) {
	            return ((MessageLite) msg).toByteArray();
        }
        if (msg instanceof MessageLite.Builder) {
            return ((MessageLite.Builder) msg).build().toByteArray();
        }
        
        throw new InvalidProtocolBufferException(msg.getClass().getName());
	}
	
	// ==== wrap-index ===== size ===========
	// totalSize 			4
	// content 				content.length
	// MAGIC_CODE 			MAGIC_CODE.length

	public byte[] encode(MessageLite msg) throws InvalidProtocolBufferException {

		byte[] content = encodeOrigin(msg);
		int totalSize = 4 + content.length + MAGIC_CODE.length;

		if (totalSize == 0 || content == null || content.length == 0)
			return null;
		
		byte[] ret = new byte[totalSize];

		System.arraycopy(ByteUtil.intToBytes(totalSize), 0, ret, 0, 4);
		System.arraycopy(content, 0, ret, 4 , content.length);
		System.arraycopy(MAGIC_CODE, 0, ret, 4 + content.length, MAGIC_CODE.length);
		return ret;
	}
	
	public byte[] encode(List<MessageLite> msgList) throws InvalidProtocolBufferException {
		
		byte[] ret = new byte[0];
		
		for (MessageLite msg : msgList) {
			byte[] buf = encode(msg);
			ret = ByteUtil.byteMerge(buf, ret);
		}
		
		if(isTcp) {
			int headLen = ret.length;
			byte[] headBuf = ByteUtil.intToBytes(headLen);
			ret = ByteUtil.byteMerge(headBuf, ret);
		}
		System.out.println("ret : "+ ret.length);
		return ret;
	}
	
}
