package com.feeyo.net.codec.http;

import java.nio.charset.Charset;
import java.util.Map;

public class HttpResponseEncoder {

	private static final Charset charset = Charset.forName("UTF-8");
	private static final String CRLF = "\r\n"; 	
	private static final String SP = " "; 		
	private static final String COLON = ":"; 		
	
	public byte[] encode(HttpResponse response) {
		
		if (response == null)
			return null;
		
		StringBuffer buf = new StringBuffer();
		
		//headline
		buf.append(response.getHttpVersion()).append(SP);
        buf.append(String.valueOf(response.getStatusCode())).append(SP);
        buf.append(response.getReasonPhrase()).append(CRLF);
	    
        
        //headers
        for (Map.Entry<String, String> h : response.headers().entrySet()) {
			buf.append(h.getKey()).append(COLON).append(SP).append(h.getValue()).append(CRLF);
		}
        
        buf.append(CRLF);
        
        //content
        byte[] content = response.getContent();
		if(content != null && content.length != 0)
			buf.append(new String(content, charset));
		
		return buf.toString().getBytes(charset);
	}
	
}
