package com.feeyo.redis.test;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TestShareMemory2 {
	
    public static void main(String[] args) throws Exception {
        RandomAccessFile f = new RandomAccessFile("/Users/zhuam/git/feeyo/feeyoredis/test.txt", "rw");
        FileChannel fc = f.getChannel();
        MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size());

        while (buf.hasRemaining()) {
            System.out.print((char)buf.get());
        }
        System.out.println();
        
        fc.close();
        f.close();
    }

}
