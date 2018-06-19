package com.feeyo.net.codec.http;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.google.protobuf.MessageLite;

/**
 *  Eraftpb.Message decoder 
 */
public class MessageDecoder implements Decoder<List<ProtobufRequest>>{
	
	private final ProtobufDecoder decoder;
	private boolean isCustomPkg;
	private List<ProtobufRequest> requestList = null;
	
	public MessageDecoder(boolean isCustomPkg) {
		super();
		this.isCustomPkg = isCustomPkg;
		this.decoder = new ProtobufDecoder(Message.getDefaultInstance(), isCustomPkg);
	}
	
	public List<ProtobufRequest> decode(byte[] data) {
		
		List<MessageLite> msgList = decoder.decode(data);
		if(msgList == null || msgList.isEmpty())
			return null;
		
		ProtobufRequest request = new ProtobufRequest(isCustomPkg, ProtobufMsgType.Erapb_Message_Type);
		request.setMsgList(msgList);
		if(requestList == null)
			requestList = new ArrayList<ProtobufRequest>(1);
		else 
			requestList.clear();
		
		requestList.add(request);
		return requestList;
	}

}
