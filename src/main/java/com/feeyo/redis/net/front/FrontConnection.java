package com.feeyo.redis.net.front;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.net.nio.Connection;

public class FrontConnection extends Connection {

	private volatile long lastTime;

	public FrontConnection(SocketChannel socketChannel) {
		super(socketChannel);
	}

	public void writeOkMessage(byte[] byteBuff) throws IOException {
		write(byteBuff);
	}

	public void writeErrMessage(String msg) {
		StringBuffer errorSB = new StringBuffer();
		errorSB.append("-ERR ");
		errorSB.append(msg);
		errorSB.append("\r\n");
		//
		write( errorSB.toString().getBytes() );
	}
	
	public long getLastTime() {
		return lastTime;
	}

	public void setLastTime(long currentTimeMillis) {
		this.lastTime = currentTimeMillis;
	}

}
