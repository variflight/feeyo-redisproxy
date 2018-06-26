package com.feeyo.net.codec.http;

public class HttpResponse extends HttpMessage{
	
	private final int statusCode;
    private final String reasonPhrase;
    
    public HttpResponse(int statusCode, String reasonPhrase) {
    	super();
    	
    	if(statusCode < 0) {
    		throw new IllegalArgumentException("status code may be not negative");
    	}
    	this.statusCode = statusCode;
    	this.reasonPhrase = reasonPhrase;
    }
    
    public HttpResponse(String httpVersion, int code, String reasonPhrase) {
    	super(httpVersion);
    	this.statusCode = code;
    	this.reasonPhrase = reasonPhrase;
    }

	public int getStatusCode() {
		return statusCode;
	}

	public String getReasonPhrase() {
		return reasonPhrase;
	}

	@Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(getHttpVersion()).append(" ");
        buf.append(statusCode).append(" ");
        buf.append(reasonPhrase).append("\r\n");
        
        byte[] content = getContent();
        if(content != null && content.length != 0)
        	buf.append(new String(content));
        return buf.toString();
    }
    
}
