package com.feeyo.net.codec.protobuf.test;

import java.nio.ByteBuffer;

import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.protobuf.ProtobufEncoder;
import com.feeyo.net.codec.protobuf.test.Eraftpb.ConfState;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Entry;
import com.feeyo.net.codec.protobuf.test.Eraftpb.EntryType;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.feeyo.net.codec.protobuf.test.Eraftpb.MessageType;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Snapshot;
import com.feeyo.net.codec.protobuf.test.Eraftpb.SnapshotMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;

public class ProtobufTest {

	public static void main(String[] args) {
		
		Entry entry = Entry.newBuilder().setContext(ByteString.copyFromUtf8("Entry0"))
				.setData(ByteString.copyFromUtf8("Entry0")).setEntryType(EntryType.EntryNormal).setEntryTypeValue(0)
				.setIndex(0).setSyncLog(false).setTerm(20)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		ConfState state = ConfState.newBuilder().addLearners(100).addLearners(101)
				.addNodes(102).addNodes(103)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		SnapshotMetadata metadata = SnapshotMetadata.newBuilder().setConfState(state)
				.setIndex(0) .setTerm(20)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		Snapshot snapshot = Snapshot.newBuilder().setData(ByteString.copyFromUtf8("snapshot"))
				.setMetadata(metadata)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		Message fromMsg = Message.newBuilder().setMsgType(MessageType.MsgAppend).setTo(9000).setFrom(1000)
				.setTerm(20).setLogTerm(30).setIndex(0).addEntries(entry).setCommit(123).setSnapshot(snapshot)
				.setReject(true).setRejectHint(1).setContext(ByteString.copyFromUtf8("message0")).build();

		ProtobufEncoder encoder = new ProtobufEncoder( true );

		ByteBuffer protobuf = encoder.encode(fromMsg);
		
		ProtobufDecoder<Message> decoder = new ProtobufDecoder<Message>(Message.getDefaultInstance(), true);
		Message toMsg = (Message) decoder.decode(protobuf.array()).get(0);
		System.out.println(toMsg.getIndex());
		
	}
}
