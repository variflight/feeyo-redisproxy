package com.feeyo.net.codec.http.test;

import java.io.IOException;

import com.feeyo.net.nio.ConnectionFactory;
import com.feeyo.net.nio.NIOAcceptor;
import com.feeyo.net.nio.NIOReactorPool;
import com.feeyo.net.nio.NetSystem;
import com.feeyo.net.nio.SystemConfig;
import com.feeyo.net.nio.buffer.BufferPool;
import com.feeyo.net.nio.buffer.bucket.BucketBufferPool;
import com.feeyo.util.ExecutorUtil;
import com.feeyo.util.Log4jInitializer;

public class HttpConnectionTest {
	
	public static void main(String[] args) throws IOException {
		
		Log4jInitializer.configureAndWatch(System.getProperty("user.dir"), "log4j.xml", 30000L);
		
		
		BufferPool bufferPool = new BucketBufferPool(1024 * 1024 * 40,  1024 * 1024 * 80, 1024 * 16,
				1024, new int[]{1024}, 1024 * 32);   
       
        
        new NetSystem(bufferPool, ExecutorUtil.create("BusinessExecutor-", 2), ExecutorUtil.create("TimerExecutor-", 2));
        
		SystemConfig systemConfig = new SystemConfig(1048576, 4194304, 4194304, 1048576);
        NetSystem.getInstance().setNetConfig( systemConfig );
		
		String name = "protobufConn";
		String bindIp = "0.0.0.0";
		int port = 8066;
		
		
		
		ConnectionFactory factory = new HttpConnectionFactory();
		NIOReactorPool reactorPool = new NIOReactorPool("nio", 1);
		
		final NIOAcceptor acceptor = new NIOAcceptor(name, bindIp, port, factory, reactorPool);
		acceptor.start();
	}

}
