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
import com.google.protobuf.MessageLite;

public class ProtobufHttpClient {

	private final Logger LOGGER = LoggerFactory.getLogger(ProtobufHttpClient.class);

	private static final String verbUserAgent = System.getProperty("java.home") + ";"
			+ System.getProperty("java.vendor") + ";" + System.getProperty("java.version") + ";"
			+ System.getProperty("user.name");
	
	private String messageType;
	
	public ProtobufHttpClient(MessageLite proto) {
		this.messageType = proto.getClass().getName();
	}

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
			con.setRequestProperty("message-type", messageType);
			con.getOutputStream().write(data, 0, data.length);
			con.getOutputStream().flush();
			int responseCode = con.getResponseCode();

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
		
		ProtobufHttpClient client = new ProtobufHttpClient(msg);
		client.post(url, protoBufs.array());
		
	}
	
}
