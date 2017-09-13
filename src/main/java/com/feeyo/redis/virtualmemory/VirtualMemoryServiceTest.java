package com.feeyo.redis.virtualmemory;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import com.feeyo.util.JavaUtils;
import com.feeyo.util.Log4jInitializer;

public class VirtualMemoryServiceTest {
	
	public static void main(String[] args) throws Exception {
		// 设置 LOG4J
		// ----------------------------------------------------------
		String path = System.getProperty("FEEYO_HOME");
		if ( path == null ) {
			path = System.getProperty("user.dir");
			if ( JavaUtils.isLinux() ) {
				path = new File( path ).getParent();
			}
		}
		
		Log4jInitializer.configureAndWatch(path, "log4j.xml", 30000L);
		
		final CountDownLatch cdl = new CountDownLatch( 30 );
		final VirtualMemoryService store = new VirtualMemoryService();
		store.start();
		
		for (int j = 0; j < 2; j++) {
			
			Thread t = new Thread( new Runnable(){

				@Override
				public void run() {

					for (int i =1;i<=100000;i++) {
						
						Message msg = new Message();
				        msg.setBody(  ("yangtao" + i).getBytes() );
				        msg.setQueueId( 0);
				        msg.setSysFlag(0);
				        msg.setBornTimestamp(System.currentTimeMillis());
						
						PutMessageResult pr = store.putMessage( msg );
						AppendMessageResult amr = pr.getAppendMessageResult();
						
						//
						Message msg1 = store.getMessage( amr.getWroteOffset(), amr.getWroteBytes() );
						System.out.println(new String( msg1.getBody() ));
						
						store.markAsConsumed(amr.getWroteOffset(), amr.getWroteBytes());
					}
					
					cdl.countDown();
				}
				
			});
			t.start();
		}
		
		cdl.await();
		System.out.println("xxxxxxxxxx");

	}
}
