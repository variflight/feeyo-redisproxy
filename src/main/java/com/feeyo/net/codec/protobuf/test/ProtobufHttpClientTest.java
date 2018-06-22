package com.feeyo.net.codec.protobuf.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.protobuf.ProtobufEncoder;
import com.feeyo.net.codec.protobuf.test.Eraftpb.ConfState;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Entry;
import com.feeyo.net.codec.protobuf.test.Eraftpb.EntryType;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.feeyo.net.codec.protobuf.test.Eraftpb.MessageType;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Snapshot;
import com.feeyo.net.codec.protobuf.test.Eraftpb.SnapshotMetadata;
import com.feeyo.util.Log4jInitializer;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.google.protobuf.UnknownFieldSet;

public class ProtobufHttpClientTest {

	private final Logger LOGGER = LoggerFactory.getLogger(ProtobufHttpClientTest.class);

	private static final String verbUserAgent = System.getProperty("java.home") + ";"
			+ System.getProperty("java.vendor") + ";" + System.getProperty("java.version") + ";"
			+ System.getProperty("user.name");

	public void get(String url) {

		HttpURLConnection con = null;
		BufferedReader br = null;
		try {

			URL urlObj = new URL(url);
			con = (HttpURLConnection) urlObj.openConnection();
			con.setUseCaches(false);
			con.setDoOutput(true);
			con.setDoInput(true);
			con.setInstanceFollowRedirects(false);
			con.setRequestMethod("GET");
			con.setConnectTimeout(2 * 1000);
			con.setReadTimeout(5 * 1000);
			con.setRequestProperty("User-Agent", verbUserAgent);
			con.setRequestProperty("Connection", "close");
			con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF8");
			con.connect();

			int responseCode = con.getResponseCode();
			StringBuffer responseTxtBuffer = new StringBuffer();
			br = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = br.readLine()) != null) {
				responseTxtBuffer.append(inputLine);
			}
			System.out.println("status: "+ responseCode + "response: " + responseTxtBuffer.toString());


		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
			if (con != null) {
				con.disconnect();
				con = null;
			}
		}

	}
	
	public void post(String url, byte[] data) {

		HttpURLConnection con = null;
		BufferedReader br = null;
		try {

			URL urlObj = new URL(url);
			con = (HttpURLConnection) urlObj.openConnection();
			con.setUseCaches(false);
			con.setDoOutput(true);
			con.setDoInput(true);
			con.setInstanceFollowRedirects(false);
			con.setConnectTimeout(2000);
			con.setReadTimeout(5000);
			con.setRequestMethod("POST");
			con.setRequestProperty("User-Agent", verbUserAgent);
			con.setRequestProperty("Connection", "close");
			con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			con.setRequestProperty("Content-Length", Integer.toString(data.length));
			con.getOutputStream().write(data, 0, data.length);
			con.getOutputStream().flush();
			con.getResponseCode();

			con.getOutputStream().close();

		} catch (IOException e) {
			LOGGER.error(e.getMessage());

		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}

			if (con != null) {
				con.disconnect();
			}
		}

	}
	
	public void post(String url, MessageLite msg, boolean isCustomPkg) {
		
		if(msg == null)
			return;
		
		ProtobufEncoder encoder = new ProtobufEncoder(isCustomPkg);
		ByteBuffer protoBufs = encoder.encode(msg);
		
		ProtobufHttpClientTest client = new ProtobufHttpClientTest();
		client.post(url, protoBufs.array());
		
	}
	
	public static void main(String[] args) {

		if (System.getProperty("FEEYO_HOME") == null) {
			System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));
		}

		// 设置 LOG4J
		Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);

		Entry entry0 = Entry.newBuilder().setContext(ByteString.copyFromUtf8("Entry" + 1))
				.setData(ByteString.copyFromUtf8("Entry " + 1)).setEntryType(EntryType.EntryNormal)
				.setEntryTypeValue(0).setIndex(1).setSyncLog(false).setTerm(20)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		ConfState state = ConfState.newBuilder().addLearners(100).addLearners(101).addNodes(102).addNodes(103)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		SnapshotMetadata metadata = SnapshotMetadata.newBuilder().setConfState(state).setIndex(1).setTerm(20)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		Snapshot snapshot = Snapshot.newBuilder().setData(ByteString.copyFromUtf8("snapshot" + 1))
				.setMetadata(metadata).setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		Message fromMsg = Message.newBuilder().setMsgType(MessageType.MsgAppend).setTo(9000).setFrom(1000).setTerm(20)
				.setLogTerm(30).setIndex(1).addEntries(entry0).setCommit(123).setSnapshot(snapshot).setReject(true)
				.setRejectHint(1).setContext(ByteString.copyFromUtf8("message" + 1)).build();
		
		
		ProtobufHttpClientTest test = new ProtobufHttpClientTest();
		test.post("http://127.0.0.1:8066/proto/eraftpb/message?isCustomPkg=true", fromMsg, true);
	}
	
}
