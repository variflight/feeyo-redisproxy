package com.feeyo.protobuf.http;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;


public class PBHttpServer {
	
	private static Logger LOGGER = LoggerFactory.getLogger(PBHttpServer.class);
	private int MAX_CONN_LIMIT = 100;
	
	private HttpServer httpServer = null;
	
	public void start(int serverPort) {
		
		try {
			httpServer = HttpServer.create(new InetSocketAddress(serverPort), MAX_CONN_LIMIT);
			httpServer.createContext("/", new PBHttpServerHandler());
			httpServer.setExecutor(null);  
	        httpServer.start();  
	        LOGGER.info("http server started");
			
		} catch (IOException e) {
			LOGGER.error("http server start fail {}", e.getMessage());
		}  
	}
	
	public void close() {
		
		if(httpServer != null) {
			//delay - the maximum time in seconds to wait until exchanges have finished.
			httpServer.stop(3);
		}
		
	}
	
}
