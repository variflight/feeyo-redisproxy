package com.feeyo.net.codec.protobuf.test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.feeyo.net.codec.http.HttpEncoder;
import com.feeyo.net.codec.http.HttpMsgType;
import com.feeyo.net.codec.http.HttpMsgRequest;
import com.feeyo.util.Log4jInitializer;

public class ProtobufClient {
	
	
	private String serverIp;
	private int port;
	
	public ProtobufClient(String serverIp, int port) {
		this.serverIp = serverIp;
		this.port = port;
	}
	
	public void write(HttpMsgRequest request ) throws UnknownHostException, IOException {
		if(request == null || request.getMsgList() == null || request.getMsgList().isEmpty())
			return;
		
		Socket socket = null;
		OutputStream out = null;
		try {
			socket = new Socket(serverIp, port);
			socket.setSoTimeout(10000);
			
			HttpEncoder encoder = new HttpEncoder();
			
			ByteBuffer buff = encoder.encode(request);
			
			out = socket.getOutputStream();
			out.write(buff.array());
		}finally {
			
			if(out != null ) {
				out.close();
			}
			
			if(socket != null) {
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
        
		ProtobufClient client = new ProtobufClient("192.168.14.158", 8066);
		
		HttpMsgRequest request = new HttpMsgRequest(true, HttpMsgType.Erapb_Message_Type);
		request.setMsgList(TestDataUtil.genBatchMessages(10));
		
		client.write(request);
    }

}
