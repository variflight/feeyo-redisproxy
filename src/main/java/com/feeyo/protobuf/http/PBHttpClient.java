package com.feeyo.protobuf.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLiteOrBuilder;

public class PBHttpClient {

	private final Logger LOGGER = LoggerFactory.getLogger(PBHttpClient.class);

	private static final String verbUserAgent = System.getProperty("java.home") + ";"
			+ System.getProperty("java.vendor") + ";" + System.getProperty("java.version") + ";"
			+ System.getProperty("user.name");

	public HttpResponse get(String url) {

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

			response = new HttpResponse(responseCode, responseTxtBuffer.toString());
			if (responseCode != HttpURLConnection.HTTP_OK) {
				LOGGER.error(responseTxtBuffer.toString());
			}

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

		return response;
	}
	
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
			con.setConnectTimeout(2000);
			con.setReadTimeout(5000);
			con.setRequestMethod("POST");
			con.setRequestProperty("User-Agent", verbUserAgent);
			con.setRequestProperty("Connection", "close");
			con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			con.setRequestProperty("Content-Length", Integer.toString(data.length));

			con.getOutputStream().write(data, 0, data.length);
			con.getOutputStream().flush();
			con.getOutputStream().close();

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
	
	public <T> void post(String url, List<T> msgList) {
		
		if(msgList == null || msgList.isEmpty())
			return;
		
		
		List<MessageLiteOrBuilder> tranMsgList = new ArrayList<MessageLiteOrBuilder>();
		for(T msg : msgList) {
			if(msg instanceof MessageLiteOrBuilder) {
				tranMsgList.add( (MessageLiteOrBuilder) msg);
			}
		}
		
		MessageWrapper wrapper = new MessageWrapper();
		try {
			byte[] protoBufs = wrapper.wrapIn(tranMsgList);
			
			PBHttpClient client = new PBHttpClient();
			HttpResponse response = client.post(url, protoBufs);
			if ( response != null && response.getCode() == 200) {
				System.out.println("send success !");
			}
			
		} catch (InvalidProtocolBufferException e1) {
			LOGGER.error("send fail");
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

}
