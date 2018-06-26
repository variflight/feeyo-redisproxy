package com.feeyo.net.codec.http;

public class HttpResponseDecoder extends HttpMessageDecoder<HttpResponse>  {


	public HttpResponseDecoder() {
		super();
	}
	
	@Override
	protected HttpResponse createMessage(String[] headline) {
		
		String httpVersion = headline[0];
		int statusCode = Integer.valueOf(headline[1]);
		String reasonPhrase = headline[2];
		
		return new HttpResponse(httpVersion, statusCode, reasonPhrase);
	}

	@Override
	protected boolean isDecodingRequest() {
		return false;
	}

}
