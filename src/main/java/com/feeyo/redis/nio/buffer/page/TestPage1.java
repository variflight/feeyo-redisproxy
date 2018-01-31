package com.feeyo.redis.nio.buffer.page;

import java.nio.ByteBuffer;

public class TestPage1 {

	public static void main(String[] args) {
		
		int allocTimes = 10240000;
//		PageBufferPool pool = new PageBufferPool(pageSize, (short) 256, (short) 100);
		ByteBufferPagePool pool = new ByteBufferPagePool(1024*100*100*10, 1024*100*200*10, 64 * 1024, 256,1024,2048);
		long start = System.currentTimeMillis();
		for (int i = 0; i < allocTimes; i++) {
			// System.out.println("allocate "+i);
			// long start=System.nanoTime();
			int size = (i % 1024) + 1;
			ByteBuffer byteBufer = pool.allocate(size);
//			ByteBuffer byteBufer2 = pool.allocate(size);
			ByteBuffer byteBufer3 = pool.allocate(size);
			// System.out.println("alloc "+size+" usage
			// "+(System.nanoTime()-start));
			// start=System.nanoTime();
			pool.recycle(byteBufer);
			pool.recycle(byteBufer3);
			// System.out.println("recycle usage "+(System.nanoTime()-start));
		}
		
		long used = (System.currentTimeMillis() - start);
		System.out.println("total used time  " + used + " avg speed " + allocTimes / used);
	}

}
