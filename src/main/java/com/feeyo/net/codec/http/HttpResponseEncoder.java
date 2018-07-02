package com.feeyo.net.codec.http;

import java.util.Map;

/**
 * 
 * Http Response encode
 * 
 * @author xuwenfeng
 *
 */
public class HttpResponseEncoder {
	
	private static final String CRLF = "\r\n"; 	
	private static final String SP = " "; 		
	private static final String COLON = ":"; 		
	
	public byte[] encode(HttpResponse response) {
		
		if ( response == null )
			return null;
		
		
		// headline
		StringBuffer buf = new StringBuffer();
		buf.append( response.getHttpVersion() ).append(SP);
        buf.append(String.valueOf(response.getStatusCode())).append(SP);
        buf.append( response.getReasonPhrase() ).append(CRLF);
	    
        
        // headers
        for (Map.Entry<String, String> h : response.headers().entrySet()) {
			buf.append(h.getKey()).append(COLON).append(SP);
			buf.append(h.getValue()).append(CRLF);
		}
        buf.append(CRLF);
        
        // content
        byte[] src1 = buf.toString().getBytes();
        byte[] src2 = response.getContent();
        
        byte[] dest = new byte[ src1.length + src2.length ]; 
        System.arraycopy(src1, 0, dest, 0, src1.length);
        System.arraycopy(src2, 0, dest, dest.length, src2.length);
 
		return dest;
	}
}
