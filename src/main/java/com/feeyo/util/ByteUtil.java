package com.feeyo.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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
	
	public static byte[] byteMerge(byte[] bt1, byte[] bt2){ 
	    byte[] ret = new byte[bt1.length+bt2.length]; 
	    System.arraycopy(bt1, 0, ret, 0, bt1.length);
	    System.arraycopy(bt2, 0, ret, bt1.length, bt2.length);
	    return ret; 
	}
	
	public static List<Integer> kmp(byte[] src, byte[] pattern){
		List<Integer> indexs = new ArrayList<Integer>();
		if(src.length <0 || pattern.length > src.length)
			return indexs;
		
		//计算next[]
		int[] next = new int[pattern.length];
        next[0] = 0;
        for(int i = 1,j = 0; i < pattern.length; i++){
            while(j > 0 && pattern[j] != pattern[i]){
                j = next[j - 1];
            }
            if(pattern[i] == pattern[j]){
                j++;
            }
            next[i] = j;
        }
        
        for(int i = 0, j = 0; i < src.length; i++){
            while(j > 0 && src[i] != pattern[j]){
                j = next[j - 1];
            }
            if(src[i] == pattern[j]){
                j++;
            }
            if(j == pattern.length){
            	indexs.add(i-j+1);
            	j = 0;
            }
        }
        return indexs;
    }
	
	public static final InputStream byte2InputStream(byte[] buf) {
		return new ByteArrayInputStream(buf);
	}
	
	public static final byte[] inputStream2byte(InputStream inStream) throws IOException {
		
		ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		int rc = 0;
		try {
			while((rc = inStream.read(buf)) > 0) {
				swapStream.write(buf,0,rc);
			}
			return swapStream.toByteArray();
		}finally {
			swapStream.close();
		}
	}
	
	public static void main(String[] args) {
		int a = -1;
		byte[] buf = ByteUtil.intToBytes(a);
		System.out.println(ByteUtil.bytesToInt(buf[3],buf[2],buf[1],buf[0]));
	}
}
