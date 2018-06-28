package com.feeyo.redis.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.feeyo.util.jedis.JedisConnection;
import com.feeyo.util.jedis.RedisCommand;
import com.feeyo.util.jedis.exception.JedisMovedDataException;

public class PerformanceTest {
	
	private static void test(int workSize, final int requestSize) {
	
		
		final AtomicInteger m5 = new AtomicInteger(0);
		final AtomicInteger m10 = new AtomicInteger(0);
		final AtomicInteger m20 = new AtomicInteger(0);
		final AtomicInteger m50 = new AtomicInteger(0);
		final AtomicInteger m50j = new AtomicInteger(0);

		final AtomicInteger sCC = new AtomicInteger(0);
		final AtomicInteger rCC = new AtomicInteger(0);

		final AtomicInteger errCC = new AtomicInteger(0);
		
		final CountDownLatch latch = new CountDownLatch( workSize);
		
		for (int i = 0; i < workSize; i++) {

			Thread t = new Thread() {
				public void run() {
					
					JedisConnection conn = null;
					try {
						conn = new JedisConnection("127.0.0.1", 8066, 2000, 0); // 2000
						conn.sendCommand(RedisCommand.AUTH, "pwd04"); // pwd01
																		// aspAuth
																		// tod_am_28192c1708028508
						conn.getBulkReply();
						
						//
						StringBuffer valueBuffer = new StringBuffer( 1024 * 320 );
						for(int v1=0; v1<1024; v1++) {
							valueBuffer.append("X11111111111111111111111111111111");
							valueBuffer.append("X11111111111111111111111111111111");
							valueBuffer.append("X11111111111111111111111111111111");
							valueBuffer.append("X11111111111111111111111111111111");
							valueBuffer.append("X11111111111111111111111111111111");
							valueBuffer.append("X11111111111111111111111111111111");
							valueBuffer.append("X11111111111111111111111111111111");
							valueBuffer.append("X11111111111111111111111111111111");
							valueBuffer.append("X11111111111111111111111111111111");
							valueBuffer.append("X11111111111111111111111111111111");
						}
						
						conn.sendCommand(RedisCommand.SET, "key1", valueBuffer.toString()  );
						try {
							Object obj = conn.getOne();
							System.out.println( (String)obj );
						} catch (JedisMovedDataException e1) {
						}

						for (int j = 0; j < requestSize; j++) {
							
							sCC.incrementAndGet();

//							StringBuffer keyBuffer = new StringBuffer();
//							keyBuffer.append(this.getName());
//							keyBuffer.append("_kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk");
//							
//							for(int x1 = 0; x1< 100; x1++) {  // 500
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//								keyBuffer.append("_000000000000000000000000000000000000000000000000000");
//							}
//							keyBuffer.append("_").append(j);
//
//							long t1 = System.currentTimeMillis();
//							conn.sendCommand(RedisCommand.GET, keyBuffer.toString());
							
							long t1 = System.currentTimeMillis();
							conn.sendCommand(RedisCommand.GET, "key1");

							try {
								Object obj = conn.getOne();
							} catch (JedisMovedDataException e1) {
							}
							// System.out.println( conn.getOne() );
							long t2 = System.currentTimeMillis();

							// System.out.println("vv:" + vv);
							long diff = t2 - t1;
							if (diff <= 5) {
								m5.incrementAndGet();
							} else if (diff > 5 && diff <= 10) {
								m10.incrementAndGet();
							} else if (diff > 10 && diff <= 20) {
								m20.incrementAndGet();
							} else if (diff > 20 && diff <= 50) {
								m50.incrementAndGet();
							} else {
								m50j.incrementAndGet();
							}

							rCC.incrementAndGet();

						}

					} catch (Exception e) {
						e.printStackTrace();
						errCC.incrementAndGet();
					} finally {
						if (conn != null) {
							conn.disconnect();
						}
					}
					
					
					latch.countDown();

				}
				
				
			};
			t.start();
		}
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("sCC:" + sCC.get() + ", rCC:" + rCC.get() + ", err:" + errCC.get());
		System.out.println("m5=" + m5.get() + ", m10=" + m10.get() + ", m20=" + m20.get() + ", m50="
				+ m50.get() + ", m50j=" + m50j.get());
		
	}

	public static void main(String[] args) {
		
		test(80, 300);
		
		try {
			Thread.sleep( 800L );
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		test(50, 1000);
		
		try {
			Thread.sleep( 800L );
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		test(100, 200);
		
		try {
			Thread.sleep( 800L );
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		test(90, 50000);
		
	}

}
