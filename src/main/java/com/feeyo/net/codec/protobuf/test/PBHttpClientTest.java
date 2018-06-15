package com.feeyo.net.codec.protobuf.test;

import java.util.List;

import com.feeyo.net.codec.protobuf.PBHttpClient;
import com.feeyo.net.codec.protobuf.Eraftpb.Message;
import com.feeyo.util.Log4jInitializer;

public class PBHttpClientTest {

	public void post(String url, List<Message> msgList) {
		PBHttpClient client = new PBHttpClient(Message.getDefaultInstance());
		client.post(url, msgList);
	}

	public static void main(String[] args) {

		if (System.getProperty("FEEYO_HOME") == null) {
			System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));
		}

		// 设置 LOG4J
		Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);

		PBHttpClientTest test = new PBHttpClientTest();
		test.post("http://192.168.14.158:8844", TestDataUtil.genBatchMessages(10));
	}
}
