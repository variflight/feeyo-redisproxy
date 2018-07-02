package com.feeyo.net.codec.http;

import com.feeyo.net.codec.util.CompositeByteArray;

public class HttpResponseEncoder  extends HttpMessageEncoder {

	@Override
	protected void encodeHeadline(CompositeByteArray buf, HttpMessage message) {
		
		HttpResponse response = (HttpResponse) message;
        buf.add(response.getHttpVersion().getBytes(charset));
        buf.add(SP);
        buf.add((String.valueOf(response.getStatusCode()).getBytes(charset)));
        buf.add(SP);
        buf.add(response.getReasonPhrase().getBytes(charset));
        buf.add(CRLF);
	}

}
