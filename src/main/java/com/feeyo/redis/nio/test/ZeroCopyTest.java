package com.feeyo.redis.nio.test;

import java.io.IOException;

import com.feeyo.redis.nio.ConnectionFactory;
import com.feeyo.redis.nio.NIOAcceptor;
import com.feeyo.redis.nio.NIOReactorPool;
import com.feeyo.util.Log4jInitializer;

public class ZeroCopyTest {
	
	public static void main(String[] args) throws IOException {
		
		Log4jInitializer.configureAndWatch(System.getProperty("user.dir"), "log4j.xml", 30000L);
		
		
		String name = "zerocopy";
		String bindIp = "0.0.0.0";
		int port = 8066;
		
		ConnectionFactory factory = new ZeroCopyConnectionFactory();
		NIOReactorPool reactorPool = new NIOReactorPool("nio", 8);
		
		final NIOAcceptor acceptor = new NIOAcceptor(name, bindIp, port, factory, reactorPool);
		acceptor.start();
	}

}
