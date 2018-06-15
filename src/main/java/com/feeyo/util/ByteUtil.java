package com.feeyo.util;

public class ByteUtil {
	
	public static int bytesToInt(byte b3, byte b2, byte b1, byte b0) {
		return   b3 & 0xFF |   
	            (b2 & 0xFF) << 8 |   
	            (b1 & 0xFF) << 16|   
	            (b0 & 0xFF) << 24;   
	}
	
	public static byte[] intToBytes(int i) {
		return new byte[] {
				(byte) ((i >> 24) & 0xFF),  
			    (byte) ((i >> 16) & 0xFF),     
			    (byte) ((i >> 8) & 0xFF),     
			    (byte) (i & 0xFF)  
		};
	}
	
}
