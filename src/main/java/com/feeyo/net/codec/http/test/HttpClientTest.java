package com.feeyo.net.codec.http.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.net.codec.http.HttpRequestEncoder;
import com.feeyo.net.codec.http.HttpResponse;
import com.feeyo.net.codec.http.HttpResponseDecoder;
import com.feeyo.net.codec.protobuf.ProtobufEncoder;
import com.feeyo.net.codec.protobuf.test.Eraftpb.ConfState;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Entry;
import com.feeyo.net.codec.protobuf.test.Eraftpb.EntryType;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.feeyo.net.codec.protobuf.test.Eraftpb.MessageType;
import com.feeyo.net.codec.protobuf.test.Eraftpb.Snapshot;
import com.feeyo.net.codec.protobuf.test.Eraftpb.SnapshotMetadata;
import com.feeyo.net.codec.util.CompositeByteArray;
import com.feeyo.util.Log4jInitializer;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;

public class HttpClientTest {

	private static final String verbUserAgent = System.getProperty("java.home") + ";"
			+ System.getProperty("java.vendor") + ";" + System.getProperty("java.version") + ";"
			+ System.getProperty("user.name");

	private String serverIp;
	private int port;

	public HttpClientTest(String serverIp, int port) {
		this.serverIp = serverIp;
		this.port = port;
	}

	public HttpResponse write(HttpRequest request) throws UnknownHostException, IOException {

		Socket socket = null;
		OutputStream out = null;
		InputStream in = null;
		try {
			socket = new Socket(serverIp, port);
			socket.setSoTimeout(10000);

			HttpRequestEncoder encoder = new HttpRequestEncoder();
			byte[] reqbuf = encoder.encode(request);

			out = socket.getOutputStream();
			out.write(reqbuf);
			out.flush();

			in = socket.getInputStream();
			CompositeByteArray buf = new CompositeByteArray();
			byte[] buff = new byte[1024];
			int len = -1;
			while ((len = in.read(buff)) != -1) {
				buf.add(Arrays.copyOfRange(buff, 0, len));
			}
			HttpResponseDecoder decoder = new HttpResponseDecoder();
			HttpResponse response = decoder.decode(buf.getData(0, buf.getByteCount()));
			return response;
		} finally {

			if (in != null) {
				in.close();
			}

			if (out != null) {
				out.close();
			}

			if (socket != null) {
				socket.close();
			}
		}
	}

	public static void main(String[] args) throws IOException {

		if (System.getProperty("FEEYO_HOME") == null) {
			System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));
		}

		// 设置 LOG4J
		Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);

		Entry entry0 = Entry.newBuilder().setContext(ByteString.copyFromUtf8("Entry" + 1))
				.setData(ByteString.copyFromUtf8("Entry " + 1)).setEntryType(EntryType.EntryNormal).setEntryTypeValue(0)
				.setIndex(1).setSyncLog(false).setTerm(20).setUnknownFields(UnknownFieldSet.getDefaultInstance())
				.build();

		ConfState state = ConfState.newBuilder().addLearners(100).addLearners(101).addNodes(102).addNodes(103)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		SnapshotMetadata metadata = SnapshotMetadata.newBuilder().setConfState(state).setIndex(1).setTerm(20)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		Snapshot snapshot = Snapshot.newBuilder().setData(ByteString.copyFromUtf8("snapshot" + 1)).setMetadata(metadata)
				.setUnknownFields(UnknownFieldSet.getDefaultInstance()).build();

		Message msg = Message.newBuilder().setMsgType(MessageType.MsgAppend).setTo(9000).setFrom(1000).setTerm(20)
				.setLogTerm(30).setIndex(1).addEntries(entry0).setCommit(123).setSnapshot(snapshot).setReject(true)
				.setRejectHint(1).setContext(ByteString.copyFromUtf8("message" + 1)).build();

		HttpClientTest client = new HttpClientTest("192.168.14.158", 8066);
		HttpRequest request = new HttpRequest("POST", "http://192.168.14.158:8066/proto/eraftpb/message");
		ProtobufEncoder encoder = new ProtobufEncoder(false);
		ByteBuffer protobuf = encoder.encode(msg);
		if (protobuf != null) {
			byte[] data = protobuf.array();
			request.setContent(data);
			request.addHeader("User-Agent", verbUserAgent);
			request.addHeader("Connection", "close");
			request.addHeader("Content-Type", "application/x-www-form-urlencoded");
			request.addHeader("Content-Length", Integer.toString(data.length));
			HttpResponse res = client.write(request);
			System.out.println(res.toString());
		}
	}

}
