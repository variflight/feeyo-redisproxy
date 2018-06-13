package com.feeyo.protobuf.http.test;

import java.util.ArrayList;
import java.util.List;

import com.feeyo.protobuf.codec.Eraftpb.ConfState;
import com.feeyo.protobuf.codec.Eraftpb.Entry;
import com.feeyo.protobuf.codec.Eraftpb.EntryType;
import com.feeyo.protobuf.codec.Eraftpb.Message;
import com.feeyo.protobuf.codec.Eraftpb.MessageType;
import com.feeyo.protobuf.codec.Eraftpb.Snapshot;
import com.feeyo.protobuf.codec.Eraftpb.SnapshotMetadata;
import com.feeyo.protobuf.http.PBHttpClient;
import com.feeyo.util.Log4jInitializer;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;

public class PBHttpClientTest {

	public Message genMessage(int index) {
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

	public void post(String url, List<Message> msgList) {
		PBHttpClient client = new PBHttpClient();
		client.post(url, msgList);
	}

	public List<Message> genBatchMessages(int count) {

		ArrayList<Message> msgList = new ArrayList<Message>();
		for (int i = 0; i < count; i++)
			msgList.add(genMessage(i));
		return msgList;
	}

	public static void main(String[] args) {

		if (System.getProperty("FEEYO_HOME") == null) {
			System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));
		}

		// 设置 LOG4J
		Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);

		PBHttpClientTest test = new PBHttpClientTest();
		List<Message> msgList = test.genBatchMessages(10);
		test.post("http://192.168.14.158:8844", msgList);
	}
}
