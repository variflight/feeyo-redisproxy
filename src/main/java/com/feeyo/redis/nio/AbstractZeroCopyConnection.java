package com.feeyo.redis.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

/**
 * Zero copy
 * 
 * @author zhuam
 *
 */
public abstract class AbstractZeroCopyConnection extends AbstractConnection {
	
	// 单通道
	protected FileChannel fileChannel;
	protected long position;
	protected long count;

	public AbstractZeroCopyConnection(SocketChannel channel) {
		super(channel);
	}

	@Override
	protected void asynRead() throws IOException {
		
		int got = (int) fileChannel.transferFrom(channel, position, count);
		switch (got) {
        case 0: 
            // not enough space
            break;
        case -1: 
        	// client closed
        	break;
        default: 
        	break;
		}
	}

	@Override
    public void write(byte[] data) {
		
		try {
			 
			fileChannel.transferTo(position, count, channel);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	@Override
    public void write(ByteBuffer data) {
		
	}
	
    @Override
	public void doNextWriteCheck() {
		// ignore
	}
	
}
