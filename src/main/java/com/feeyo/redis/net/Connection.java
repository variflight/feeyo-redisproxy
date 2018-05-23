package com.feeyo.redis.net;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.feeyo.redis.nio.AbstractConnection;

public class Connection extends AbstractConnection {

	private volatile long lastTime;
	
	protected String password;

	public Connection(SocketChannel channel) {
		super(channel);
	}

	public void writeOkMessage(byte[] byteBuff) throws IOException {
		write( byteBuff );
	}
	
	public void writeErrMessage(String msg) {
		StringBuffer errorBuffer = new StringBuffer();
		errorBuffer.append("-ERR ");
		errorBuffer.append( msg );
		errorBuffer.append("\r\n");
		
		write( errorBuffer.toString().getBytes() );
	}
	
	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public long getLastTime() {
		return lastTime;
	}
	
	public void setLastTime(long currentTimeMillis) {
		this.lastTime = currentTimeMillis;
	}
	
}