package com.feeyo.redis.test;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TestShareMemory1 {
	
	public static void main(String args[]){
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile("/Users/zhuam/git/feeyo/feeyoredis/test.txt", "rw");
            FileChannel fc = f.getChannel();
            MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, 200);

            buf.put("how are you?".getBytes());

            Thread.sleep(10000);

            fc.close();
            f.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
