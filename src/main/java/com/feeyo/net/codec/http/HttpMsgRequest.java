package com.feeyo.net.codec.http;

import java.util.List;

import com.google.protobuf.MessageLite;

public class HttpMsgRequest {
	
	private final boolean isCustomPkg;
	private final int protoType;			//根据protoType 选择对应接口
	private List<MessageLite> msgList;
	
	public HttpMsgRequest(boolean isCustomPkg, final int protoType) {
		this.isCustomPkg = isCustomPkg;
		this.protoType = protoType;
	}
	
	public boolean isCustomPkg() {
		return isCustomPkg;
	}
	
	public int getProtoType() {
		return protoType;
	}

	public List<MessageLite> getMsgList() {
		return msgList;
	}
	
	public void setMsgList(List<MessageLite> msgList) {
		this.msgList = msgList;
	}
	
	public int getNormalSize() {
		
		if(msgList == null || msgList.isEmpty())
			return 0;
		
		int size = 0;
		for(MessageLite msg : msgList) {
			size += msg.getSerializedSize();
		}
		
		//
		if(isCustomPkg) {
			size += 4 * msgList.size();
		}
		
		size += 5;	//prototype 4 + isCustomPkg 1
		return size ;
	}
	
	
	
}
