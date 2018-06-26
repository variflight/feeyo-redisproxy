package com.feeyo.net.codec.http;

public class HttpRequest extends HttpMessage{

	private final String method;
	private final String uri;

	public HttpRequest(String method, String uri) {
		super();
		if(method == null || uri == null) {
			throw new IllegalArgumentException("Http method or uri may be not null");
		}
		this.method = method;
		this.uri = uri;
	}
	
	public HttpRequest(String httpVersion, String method, String uri) {
		super(httpVersion);
		this.method = method;
		this.uri = uri;
	}

	public String getMethod() {
		return method;
	}

	public String getUri() {
		return uri;
	}
	
}
