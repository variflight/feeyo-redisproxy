package com.feeyo.net.codec.protobuf.test;

import com.feeyo.net.codec.protobuf.PBHttpServer;
import com.feeyo.util.Log4jInitializer;

public class PBHttpServerTest {

	public static void main(String[] args) {

		if (System.getProperty("FEEYO_HOME") == null) {
			System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));
		}

		// 设置 LOG4J
		Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);
		
		PBHttpServer server = new PBHttpServer();
		server.start(8844);
	}

}
