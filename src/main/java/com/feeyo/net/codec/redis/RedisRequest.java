package com.feeyo.net.codec.redis;

import java.nio.ByteBuffer;

public class RedisRequest {
	
	private static RedisRequestEncoder encode = new RedisRequestEncoder();

	private byte[][] args;
	private boolean inline = false;
	
	private RedisRequestPolicy policy;


	public byte[][] getArgs() {
		return args;
	}

	public void setArgs(byte[][] args) {
		this.args = args;
	}
	
	public int getNumArgs(){
		if ( args == null )
			return 0;
		else
			return args.length;
	}
	
	public boolean isInline() {
		return inline;
	}

	public void setInline(boolean inline) {
		this.inline = inline;
	}

	public int getSize() {
		int size = 0;
		if ( args != null ) {
			for(byte[] arg: args) {
				size = size + arg.length;
			}
		}
		return size;		
	}

	public ByteBuffer encode() {
		ByteBuffer buffer = encode.encode(args);
		return buffer;			
	}
	
	public void clear() {
		args = null;
	}
	
	public RedisRequestPolicy getPolicy() {
		return policy;
	}

	public void setPolicy(RedisRequestPolicy policy) {
		this.policy = policy;
	}

	@Override
	public String toString() {
		StringBuffer sBuffer = new StringBuffer();
		sBuffer.append("\r\n");
		sBuffer.append("inline=").append( inline ).append("\r\n");
		if ( args != null ) {
			for(byte[] arg: args) {
				sBuffer.append("arg=").append( arg != null ? new String(arg) : null ).append("\r\n");
			}
		}
		return sBuffer.toString();
	}
}
