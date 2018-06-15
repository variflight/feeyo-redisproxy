package com.feeyo.net.codec.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.protobuf.Eraftpb.Message;
import com.feeyo.util.ByteUtil;
import com.feeyo.util.Log4jInitializer;
import com.feeyo.util.ThreadFactoryImpl;
import com.google.protobuf.MessageLite;

public class PBTcpServer {
	
	private final Logger LOGGER = LoggerFactory.getLogger(PBTcpServer.class);
	private ThreadPoolExecutor threadPoolExecutor;
	private int corePoolSize = 10;
	private int maxPoolSize = 20;
	private long keepAliveTime = 1000;
	
	private ServerSocket serverSocket = null;
	
	public PBTcpServer() {
		this.threadPoolExecutor = new ThreadPoolExecutor(
				corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(20), new ThreadFactoryImpl("ProtoTcpServer"));
		this.threadPoolExecutor.prestartAllCoreThreads();
	}
	
	
	public void start(int port) {
		
		try {
			
			serverSocket = new ServerSocket(port);
			Socket socket = serverSocket.accept();
			threadPoolExecutor.execute(new ServerThread(socket, Message.getDefaultInstance()));
			
		} catch (IOException e) {
			LOGGER.error("tcp-server startup failed");
		}
		
	}
	
	public void close() {
		
		if(serverSocket != null) {
			try {
				serverSocket.close();
			} catch (IOException e) {
			}
		}
	}
	
	public class ServerThread implements Runnable{
		
		private Socket socket = null;
		private PBDecoder decoder = null;
		
		public ServerThread(Socket socket, MessageLite proto) {
			this.socket = socket;
			this.decoder = new PBDecoder(proto, true);
		}

		@Override
		public void run() {
			
			try {
				InputStream in = socket.getInputStream();
				while(true) {
					byte[] buff = ByteUtil.inputStream2byte(in);
					List<MessageLite> msgList = decoder.decode(buff);
					if(msgList != null) {
						System.out.println("receive msg length -> "+ msgList.size());
						return;
					}else {
						System.out.println("data not enough");
					}
				}
				
			}catch(IOException e) {
				LOGGER.error(e.getMessage());
			} finally{
				if(socket != null) {
					try {
						socket.close();
					} catch (IOException e) {
					}
				}
			}
			
		}
	}
	
	public static void main(String[] agrs) {
		
		if (System.getProperty("FEEYO_HOME") == null) {
			System.setProperty("FEEYO_HOME", System.getProperty("user.dir"));
		}

		// 设置 LOG4J
		Log4jInitializer.configureAndWatch(System.getProperty("FEEYO_HOME"), "log4j.xml", 30000L);
		
		PBTcpServer server = new PBTcpServer();
		server.start(8844);
		
	}

}
