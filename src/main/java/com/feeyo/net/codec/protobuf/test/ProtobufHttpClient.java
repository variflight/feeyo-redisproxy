package com.feeyo.net.codec.protobuf.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.http.HttpEncoder;
import com.feeyo.net.codec.http.ProtobufMsgType;
import com.feeyo.net.codec.http.ProtobufRequest;
import com.feeyo.util.Log4jInitializer;

public class ProtobufHttpClient {

	private final Logger LOGGER = LoggerFactory.getLogger(ProtobufHttpClient.class);

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
			con.setRequestProperty("Connection", "close");
			con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			con.setRequestProperty("Content-Length", Integer.toString(data.length));
			con.getOutputStream().write(data, 0, data.length);
			con.getOutputStream().flush();
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
	
	public void post(String url,ProtobufRequest request) {
		
		if(request == null || request.getMsgList() == null || request.getMsgList().isEmpty())
			return;
		
		HttpEncoder encoder = new HttpEncoder();
		ByteBuffer reqBuff = encoder.encode(request);
		
		post(url, reqBuff.array());
		
	}
	
	public static void main(String[] args) {

		if (System.getProperty("FEEYO_HOME") == null) {
			System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));
		}

		// 设置 LOG4J
		Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);

		ProtobufHttpClient client = new ProtobufHttpClient();
		ProtobufRequest request = new ProtobufRequest(true, ProtobufMsgType.Erapb_Message_Type);
		request.setMsgList(TestDataUtil.genBatchMessages(10));
		
		client.post("http://192.168.14.158:8066",request);
	}

}
