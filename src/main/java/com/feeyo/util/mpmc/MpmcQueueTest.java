package com.feeyo.util.mpmc;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class MpmcQueueTest {
	
	private static AtomicInteger atomicINT = new AtomicInteger(0);
	private static CountDownLatch latch = new CountDownLatch(60);
	
	final static int SIZE = 1000000;
	final static MpmcQueue<Integer>  queue = new MpmcQueue<Integer>(SIZE);
	//final static ConcurrentLinkedQueue<Integer>  queue = new ConcurrentLinkedQueue<Integer>();
	
	static class Producer extends Thread {
		public void run() {
			
			for(;;) {
				int v = atomicINT.incrementAndGet();
				if ( v > SIZE ) {
					latch.countDown();
					return;
				}
				boolean isAdded =queue.offer( v );
				//System.out.println( isAdded );
			}
		}
	}
	
	static class Consumer extends Thread {
		
		public void run() {
			while( !queue.isEmpty() ) {
				
				Integer obj = queue.poll();
				//System.out.println("#### " + obj + ", " + queue.size() );
				
			}
			
			latch.countDown();
		}
		
	}
	
	public static void main(String[] args) {
		
		long mill1 = System.currentTimeMillis();
		for( int j1 = 0; j1 < 30; j1++) {
			Producer p = new Producer();
			p.start();
		}
		
		for(int j = 0; j < 30; j++) {
			
			Consumer c = new Consumer();
			c.start();
		}
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		long mill2 = System.currentTimeMillis();
		
		System.out.println( "cost="+ (mill2 - mill1) + ", queue info=" + queue.size()  + ", " + queue.isEmpty() );
	}

}
