package com.feeyo.net.nio.buffer.page;

import java.nio.ByteBuffer;

public class TestPage2 {
	
	public static void main(String[] args) {
		
		
	        PageBufferPool pool = new PageBufferPool(1024*100*100*10, 1024*100*200*10, 64 * 1024, 256, new int[] {1024},2048);
	        ByteBuffer byteBuffer = pool.allocate(1024);
	        String str = "DirectByteBufferPool pool = new DirectByteBufferPool(pageSize, (short) 256, (short) 8)";
	        ByteBuffer newByteBuffer = null;
	        int i = 0;
	        while (i<10){
	            if(byteBuffer.remaining()<str.length()){
	                newByteBuffer = pool.expandBuffer(byteBuffer);
	                byteBuffer = newByteBuffer;
	                i++;
	            }else {
	                byteBuffer.put(str.getBytes());
	            }
	            System.out.println("remaining: " +  byteBuffer.remaining() + "capacity: " + byteBuffer.capacity());
	        }

	        System.out.println("capacity : " + byteBuffer.capacity());
	        System.out.println("capacity : " + byteBuffer.position());

	        byte [] bytes = new byte[byteBuffer.position()];
	        byteBuffer.flip();
	        byteBuffer.get(bytes);
	        String body = new String(bytes);

	        System.out.println(byteBuffer.position());

	        System.out.println(body);

	        System.out.println("size :" + body.length());
	        pool.recycle(byteBuffer);
	}

}
