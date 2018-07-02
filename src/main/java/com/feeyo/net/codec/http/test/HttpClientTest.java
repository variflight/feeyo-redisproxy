package com.feeyo.net.codec.http.test;
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
import com.google.protobuf.UnknownFieldSet;

public class HttpClientTest {

	private final Logger LOGGER = LoggerFactory.getLogger(HttpClientTest.class);

	private static final String verbUserAgent = System.getProperty("java.home") + ";"
			+ System.getProperty("java.vendor") + ";" + System.getProperty("java.version") + ";"
			+ System.getProperty("user.name");
	
	public HttpResponse post(String url, byte[] data) {

		HttpResponse response = null;
		HttpURLConnection con = null;
		BufferedReader br = null;
		try {

			URL urlObj = new URL(url);
			con = (HttpURLConnection) urlObj.openConnection();
			con.setUseCaches(false);
			con.setDoOutput(true);
			con.setDoInput(true);
			con.setInstanceFollowRedirects(false);
			con.setConnectTimeout(10000);
			con.setReadTimeout(5000);
			con.setRequestMethod("POST");
			con.setRequestProperty("User-Agent", verbUserAgent);
			con.setRequestProperty("Connection", "close");
			con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			con.setRequestProperty("Content-Length", Integer.toString(data.length));
			con.getOutputStream().write(data, 0, data.length);
			con.getOutputStream().flush();

			int responseCode = con.getResponseCode();
			StringBuffer responseTxtBuffer = new StringBuffer();
			br = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = br.readLine()) != null) {
				responseTxtBuffer.append(inputLine);
			}

			response = new HttpResponse(responseCode, responseTxtBuffer.toString());

			if (responseCode != HttpURLConnection.HTTP_OK) {
				LOGGER.error(responseTxtBuffer.toString());
			}

		} catch (IOException e) {
			e.printStackTrace();
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

		return response;
	}
	
	public  void post(String url, Message msg) {
		
		if(msg == null )
			return;
		
		ProtobufEncoder encoder = new ProtobufEncoder(false);
		ByteBuffer protoBufs = encoder.encode(msg);
		HttpResponse response = post(url, protoBufs.array());
		if ( response != null && response.getCode() == 200) {
			System.out.println("send success !");
			System.out.println(response.getResponseTxt());
		}
		
	}
	
	public class HttpResponse {
		private int code;
		private String responseTxt;

		public HttpResponse(int code, String responseTxt) {
			super();
			this.code = code;
			this.responseTxt = responseTxt;
		}

		public int getCode() {
			return code;
		}

		public String getResponseTxt() {
			return responseTxt;
		}
	}
	
	public static void main(String[] args) {

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

		HttpClientTest test = new HttpClientTest();
		test.post("http://192.168.14.158:8066/proto/eraftpb/message", msg);
	}

}
