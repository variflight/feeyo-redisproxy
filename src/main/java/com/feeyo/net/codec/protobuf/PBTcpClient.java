package com.feeyo.net.codec.protobuf;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.feeyo.net.codec.protobuf.test.TestDataUtil;
import com.feeyo.util.Log4jInitializer;
import com.google.protobuf.MessageLite;

public class PBTcpClient {
	
	
	private String serverIp;
	private int port;
	
	public PBTcpClient(String serverIp, int port) {
		this.serverIp = serverIp;
		this.port = port;
	}
	
	public <T> void write(List<T> msgList ) throws UnknownHostException, IOException {
		if(msgList == null || msgList.isEmpty())
			return;
		
		
		List<MessageLite> tranMsgList = new ArrayList<MessageLite>();
		for(T msg : msgList) {
			if(msg instanceof MessageLite) {
				tranMsgList.add( (MessageLite) msg);
			}
		}
		Socket socket = null;
		OutputStream out = null;
		try {
			socket = new Socket(serverIp, port);
			socket.setSoTimeout(10000);
			
			PBEncoder encoder = new PBEncoder(true);
			byte[] protobuf = encoder.encode(tranMsgList);
			
			out = socket.getOutputStream();
			out.write(protobuf);
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
        
		PBTcpClient client = new PBTcpClient("192.168.14.158", 8844);
		client.write(TestDataUtil.genBatchMessages(100000));
    }  

}
