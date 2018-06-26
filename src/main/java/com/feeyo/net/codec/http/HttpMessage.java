package com.feeyo.net.codec.http;

import java.util.HashMap;
import java.util.Iterator;

public abstract class HttpMessage {
	
	public static final String HTTP_1_1 = "http/1.1";
	public static final String HTTP_1_0 = "http/1.0";
	
	private HashMap<String, String> headers;
	private String httpVersion;
	private byte[] content = null;
	
	public HttpMessage() {
		this(HTTP_1_1);
	}
	
	public HttpMessage(String httpVersion) {
		
		if(!(HTTP_1_1.equalsIgnoreCase(httpVersion) || HTTP_1_0.equalsIgnoreCase(httpVersion))) {
			throw new IllegalArgumentException("Unsupported http protocol version, excepted http/1.1 or http/1.0 ");
		}
		
		this.httpVersion = httpVersion;
		this.headers = new HashMap<String, String>();
	}
	
	public void addHeader(String headName, String headValue) {
		this.headers.put(headName, headValue);
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public HashMap<String, String> headers() {
		return headers;
	}
	
	public boolean containsHeader(String header) {
		return headers.containsKey(header);
	}
	
    public boolean containsHeader(String header, String value, boolean ignoreCase) {
    	
        Iterator<String> valueIterator = headers.values().iterator();
        if (ignoreCase) {
            while (valueIterator.hasNext()) {
                if (valueIterator.next().equalsIgnoreCase(value)) {
                    return true;
                }
            }
        } else {
            while (valueIterator.hasNext()) {
                if (valueIterator.next().equals(value)) {
                    return true;
                }
            }
        }
        return false;
    }

	public String getHttpVersion() {
		return httpVersion;
	}

}
