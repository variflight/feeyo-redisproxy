package com.feeyo.net.codec.http;

public class HttpRequest {

	private String method;
	private String uri;

	private byte[] data;

	public HttpRequest(String method, String uri) {
		super();
		this.method = method;
		this.uri = uri;
	}

	public String getMethod() {
		return method;
	}

	public String getUri() {
		return uri;
	}
	
	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

}
