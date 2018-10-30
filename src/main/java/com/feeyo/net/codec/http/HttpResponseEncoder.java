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
	
	
	private String encodeHeader(HttpResponse response) {
		
		// headline
		StringBuffer headerSb = new StringBuffer();
		headerSb.append( response.getHttpVersion() ).append(SP);
        headerSb.append( String.valueOf( response.getStatusCode() ) ).append(SP);
        headerSb.append( response.getReasonPhrase() ).append(CRLF);
        
       
		   
        // headers
        for (Map.Entry<String, String> h : response.headers().entrySet()) {
			headerSb.append(h.getKey()).append(COLON).append(SP);
			headerSb.append(h.getValue()).append(CRLF);
		}
        headerSb.append(CRLF);
        
        return headerSb.toString();
		
	}
	
	public byte[] encode(HttpResponse response) {
		
		if ( response == null )
			return null;
		
	    // body
        byte[] body = response.getContent();
		
		// header
		response.headers().put(HttpHeaderNames.CONTENT_LENGTH, String.valueOf( body == null ? 0 : body.length ) );
    	byte[] header = this.encodeHeader( response ).getBytes();
		
        if( body != null ) {
        	byte[] dest = new byte[ header.length + body.length  ]; 
	        System.arraycopy(header, 0, dest, 0, header.length);
	        System.arraycopy(body, 0, dest, header.length, body.length);
	        return dest;
	        
        } else {
        	return header;
        }
 
	}
}
