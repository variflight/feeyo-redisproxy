package com.feeyo.protobuf.http.test;

import java.lang.reflect.InvocationTargetException;

import com.feeyo.protobuf.codec.Eraftpb.ConfState;
import com.feeyo.protobuf.codec.Eraftpb.Entry;
import com.feeyo.protobuf.codec.Eraftpb.EntryType;
import com.feeyo.protobuf.codec.Eraftpb.Message;
import com.feeyo.protobuf.codec.Eraftpb.MessageType;
import com.feeyo.protobuf.codec.Eraftpb.Snapshot;
import com.feeyo.protobuf.codec.Eraftpb.SnapshotMetadata;
import com.feeyo.protobuf.codec.PBDecoder;
import com.feeyo.protobuf.codec.PBEncoder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;

public class ProtoBufTest {

	public static void main(String[] args) {

		try {
			Entry entry = Entry.newBuilder().setContext(ByteString.copyFromUtf8("Entry0"))
					.setData(ByteString.copyFromUtf8("Entry0")).setEntryType(EntryType.EntryNormal).setEntryTypeValue(0)
					.setIndex(0)
					.setSyncLog(false).setTerm(20)
					.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();
	
			ConfState state = ConfState.newBuilder().addLearners(100).addLearners(101)
					.addNodes(102).addNodes(103)
					.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();
	
			SnapshotMetadata metadata = SnapshotMetadata.newBuilder().setConfState(state)
					.setIndex(0)
					.setTerm(20)
					.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();
	
			Snapshot snapshot = Snapshot.newBuilder().setData(ByteString.copyFromUtf8("snapshot"))
					.setMetadata(metadata)
					.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();
	
			Message fromMsg = Message.newBuilder().setMsgType(MessageType.MsgAppend).setTo(9000).setFrom(1000)
					.setTerm(20).setLogTerm(30).setIndex(0).addEntries(entry).setCommit(123).setSnapshot(snapshot)
					.setReject(true).setRejectHint(1).setContext(ByteString.copyFromUtf8("message0")).build();
	
			PBEncoder encoder = new PBEncoder();
	
			byte[] protobuf;
			protobuf = encoder.encode(fromMsg);
	
			PBDecoder decoder = new PBDecoder(Message.class);
			Message toMsg = (Message) decoder.decode(protobuf);
			System.out.println(toMsg.getIndex());
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
}
