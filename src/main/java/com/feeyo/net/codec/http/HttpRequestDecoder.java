package com.feeyo.net.codec.http;

public class HttpRequestDecoder extends HttpMessageDecoder<HttpRequest> {

	public HttpRequestDecoder() {
		super();
	}

	@Override
	protected HttpRequest createMessage(String[] headline) {
		
		String method = headline[0];
		String uri = headline[1];
		String protocol = headline[2];
		return new HttpRequest(protocol, method, uri);
	}

	@Override
	protected boolean isDecodingRequest() {
		return true;
	}

}