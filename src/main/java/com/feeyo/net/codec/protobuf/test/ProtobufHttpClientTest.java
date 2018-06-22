package com.feeyo.net.codec.protobuf.test;

import com.feeyo.net.codec.protobuf.test.Eraftpb.Message;
import com.feeyo.util.Log4jInitializer;
import com.google.protobuf.MessageLite;

public class ProtobufHttpClientTest {

	public void post(String url, MessageLite msg, boolean isCustomPkg) {
		ProtobufHttpClient client = new ProtobufHttpClient(Message.getDefaultInstance());
		client.post(url, msg, isCustomPkg);
	}

	public static void main(String[] args) {

		if (System.getProperty("FEEYO_HOME") == null) {
			System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));
		}

		// 设置 LOG4J
		Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);

		ProtobufHttpClientTest test = new ProtobufHttpClientTest();
		test.post("http://127.0.0.1:8066/proto/eraftpb/message?isCustomPkg=false", TestDataUtil.genMessage(1), false);
	}
}
