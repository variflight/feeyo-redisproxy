package com.feeyo.net.codec.http;

import com.feeyo.net.codec.util.CompositeByteArray;

public class HttpRequestEncoder extends HttpMessageEncoder {

	private static final char SLASH = '/';
    private static final char QUESTION_MARK = '?';
    
	@Override
	protected void encodeHeadline(CompositeByteArray buf, HttpMessage message) {
		HttpRequest request = (HttpRequest) message;
		buf.add(request.getMethod().getBytes(charset));
		buf.add(SP);
		
		String uri = request.getUri();
		
        int start = uri.indexOf("://");
        if (start != -1) {
        	
            int startIndex = uri.indexOf(SLASH, start + 3);
            if(startIndex == -1) {
            	throw new RuntimeException("Http uri may out of form ");
            }else {
            	int index = uri.indexOf(QUESTION_MARK, startIndex);
            	uri = index == -1 ? uri.substring(startIndex) : uri.substring(startIndex, index);
            }
        }
        
		buf.add(uri.getBytes(charset));
		buf.add(SP);
		buf.add(request.getHttpVersion().getBytes(charset));
		buf.add(CRLF);
	}
	
}
