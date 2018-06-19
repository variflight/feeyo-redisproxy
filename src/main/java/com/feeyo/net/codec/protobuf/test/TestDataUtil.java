package com.feeyo.net.codec.protobuf.test;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.net.codec.protobuf.test.Eraftpb.ConfState;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Entry;
import com.feeyo.net.codec.protobuf.test.Eraftpb.EntryType;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.feeyo.net.codec.protobuf.test.Eraftpb.MessageType;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Snapshot;
import com.feeyo.net.codec.protobuf.test.Eraftpb.SnapshotMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.google.protobuf.UnknownFieldSet;

public class TestDataUtil {
	
	public static List<MessageLite> genBatchMessages(int count) {

		ArrayList<MessageLite> msgList = new ArrayList<MessageLite>();
		for (int i = 0; i < count; i++)
			msgList.add(genMessage(i));
		return msgList;
	}
	
	public static Message genMessage(int index) {
		Entry entry0 = Entry.newBuilder().setContext(ByteString.copyFromUtf8("Entry" + index))
				.setData(ByteString.copyFromUtf8("Entry " + index)).setEntryType(EntryType.EntryNormal)
				.setEntryTypeValue(0).setIndex(index).setSyncLog(false).setTerm(20)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		ConfState state = ConfState.newBuilder().addLearners(100).addLearners(101).addNodes(102).addNodes(103)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		SnapshotMetadata metadata = SnapshotMetadata.newBuilder().setConfState(state).setIndex(index).setTerm(20)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		Snapshot snapshot = Snapshot.newBuilder().setData(ByteString.copyFromUtf8("snapshot" + index))
				.setMetadata(metadata).setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		Message fromMsg = Message.newBuilder().setMsgType(MessageType.MsgAppend).setTo(9000).setFrom(1000).setTerm(20)
				.setLogTerm(30).setIndex(index).addEntries(entry0).setCommit(123).setSnapshot(snapshot).setReject(true)
				.setRejectHint(1).setContext(ByteString.copyFromUtf8("message" + index)).build();
		return fromMsg;
	}

}
