package com.feeyo.redis.nio.buffer.bucket;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;


public class TestBucket1 {

	// new DirectByteBufferPool(64,2048,64*1024, 500)
	public static void main(String[] args) {

//		final ByteBufferBucketPool bufferPool = new ByteBufferBucketPool(1024*1024 * 15, 50 * 1024 * 1024, 64 * 1024, 128, new int[] {1024}, 64 * 1024);
		final ByteBufferBucketPool bufferPool = new ByteBufferBucketPool(1207959552,1207959552,65536,0,new int[] {1024, 1024, 2048, 4096, 4096, 4096, 8192, 8192, 8192, 8192, 8192, 8192},65536, 4);
		AbstractByteBufferBucket[] buckets = bufferPool.buckets();

//		ByteBuffer buffer1 = bufferPool.allocate(100);
//		ByteBuffer buffer2 = bufferPool.allocate(1024);
//		ByteBuffer buffer3 = bufferPool.allocate(4096);
//		ByteBuffer buffer4 = bufferPool.allocate(8192);
//		ByteBuffer buffer5 = bufferPool.allocate(8192 * 2);
//		ByteBuffer buffer6 = bufferPool.allocate(8192 * 3);
//		ByteBuffer buffer7 = bufferPool.allocate(8192 * 6);
//		
//		bufferPool.recycle(buffer1);
//		bufferPool.recycle(buffer2);
//		bufferPool.recycle(buffer3);
//		bufferPool.recycle(buffer4);
//		bufferPool.recycle(buffer5);
//		bufferPool.recycle(buffer6);
//		bufferPool.recycle(buffer7);
		
		
		long t1 = System.currentTimeMillis();
		
		
		int size = 24;
		final CountDownLatch c = new CountDownLatch( size );
		
		for(int i= 0; i < size; i++) {
			Thread t = new Thread("$_" + i) {
				public void run() {
					for (int j = 0; j <= 1000000; j++) {
//						int chunkSize = 1000 + j;
//						if ( chunkSize > 1024 ) {
//							chunkSize = 1000;
//						}
						int chunkSize = 1000;
						
						ByteBuffer b = bufferPool.allocate( chunkSize );
//						if ( b == null) {
//							System.out.println("b is null + " + j);
//						} else {
//							if (b.position() > 0) {
//								System.err.println("eeeeeeeeeeeee " + b.isDirect() );
//							}
//						}
//						
//						String t = "b is null + ";
//						b.put( t.getBytes() );
						bufferPool.recycle(b);
					}
					c.countDown();
					
				}
			};
			t.start();
		}

		try {
			c.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		long t2 = System.currentTimeMillis();
		System.out.println("t2-t1:" + (t2-t1));

		int count = 0;
		for (AbstractByteBufferBucket bucket : buckets) {
			count += bucket.getCount();
		}
		System.out.println(count);
		
	}

}
