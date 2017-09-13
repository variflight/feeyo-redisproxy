package com.feeyo.redis.test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class TestSlow {
	
	public static void main(String[] args) {
		
		/*
		  	2a 32 0d 0a 24 34 0d 0a     * 2 . . $ 4 . . 
			41 55 54 48 0d 0a 24 35     A U T H . . $ 5 
			0d 0a 70 77 64 30 31 0d     . . p w d 0 1 . 
			0a 2a 33 0d 0a 24 33 0d     . * 3 . . $ 3 . 
			0a 53 45 54 0d 0a 24 31     . S E T . . $ 1 
			36 0d 0a 6b 65 79 3a 5f     6 . . k e y : _ 
			5f 72 61 6e 64 5f 69 6e     _ r a n d _ i n 
			74 5f 5f 0d 0a 24 33 0d     t _ _ . . $ 3 . 
			0a 78 78 78 0d 0a           . x x x . . 
		 */
		 byte[] bb = "*2\r\n$4\r\nAUTH\r\n$5\r\npwd01\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:__rand_int__\r\n$3\r\nxxx\r\n".getBytes();
		 int bb_read = 0;
		
		try {

			// 1、创建客户端Socket，指定服务器地址和端口
			Socket socket = new Socket("127.0.0.1", 8066);
			BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());
			
			for(int i = 0; i < bb.length; i++) {
				if (i != 0 && i % 2 == 0 ) {
					
					byte[] bb1 = new byte[i-bb_read];
					System.arraycopy(bb, bb_read, bb1, 0, bb1.length);
					
					bos.write(bb1);// 发送数据包
					bos.flush();
					
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
					}
					
					bb_read = i;
				}
				
			}
			
			if ( bb_read < bb.length ) {
				byte[] bb2 = new byte[bb.length - bb_read];
				System.arraycopy(bb, bb_read, bb2, 0, bb2.length);
				
				bos.write(bb2);// 发送数据包
				bos.flush();
			}
			
			
			
			byte[] result = new byte[1024];
			BufferedInputStream bin = new BufferedInputStream(socket.getInputStream());
			bin.read(result);
			
			System.out.println(new String(result));
			
			
			//
			
			bos.write("*2\r\n$3\r\nGET\r\n$2\r\nAA\r\n".getBytes());// 发送数据包
			bos.flush();
			
			
			byte[] result2 = new byte[1024];
			BufferedInputStream bin2 = new BufferedInputStream(socket.getInputStream());
			bin2.read(result2);
			
			System.out.println(new String(result2));
			
			socket.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

}
