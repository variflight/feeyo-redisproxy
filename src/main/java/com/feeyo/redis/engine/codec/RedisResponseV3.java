package com.feeyo.redis.engine.codec;

public class RedisResponseV3 {

	private byte type;
	private Object data;

	public RedisResponseV3(byte type, Object data) {
		this.type = type;
		this.data = data;
	}

	public RedisResponseV3(byte type, int size) {
		this.type = type;
		this.data = new RedisResponseV3[size];
	}

	public void set(int pos, RedisResponseV3 reply) {
		((RedisResponseV3[]) data)[pos] = reply;
	}

	public boolean is(byte b) {
		return type == b;
	}
	
	public byte type() {
		return type;
	}

	public Object data() {
		return data;
	}
	
	public void clear() {
		this.data = null;
	}

	//
	public String toString() {
		StringBuffer s = toString( new StringBuffer(), this );
		return s.toString();
	}
	
	private StringBuffer toString(StringBuffer sBuffer, RedisResponseV3 item) {
		
		if ( item == null ) {
			sBuffer.append("null").append("\r\n");
			return sBuffer;
		}
		
		if ( item.type == '+' ) {
			sBuffer.append("type=").append( (char)item.type ).append(", ");
			sBuffer.append("data=").append( new String( (byte[])item.data) ).append("\r\n");
			
		} else if ( item.type == '-') {
			sBuffer.append("type=").append( (char)item.type ).append(", ");
			sBuffer.append("data=").append( new String( (byte[])item.data) ).append("\r\n");
			
		} else if ( item.type == ':') {
			sBuffer.append("type=").append( (char)item.type ).append(", ");
			sBuffer.append("data=").append( new String( (byte[])item.data ) ).append("\r\n");;
			
		} else if ( item.type == '$') {
			sBuffer.append("type=").append( (char)item.type ).append(", ");
			sBuffer.append("data=").append( new String( (byte[])item.data ) ).append("\r\n");;
			
		} else if ( item.type == '*') {				
			if  ( item.data instanceof byte[] ) {				
				sBuffer.append("type=").append( (char)item.type ).append(", ");
				sBuffer.append("data=").append( new String( (byte[])item.data ) ).append("\r\n");				
			} else {
				RedisResponseV3[] items = (RedisResponseV3[]) item.data;
				for(int i = 0; i < items.length; i++) {
					toString(sBuffer, items[i] );
				}
			}
		}
		return sBuffer;
	}
}