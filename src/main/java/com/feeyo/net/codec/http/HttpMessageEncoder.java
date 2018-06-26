package com.feeyo.net.codec.http;

import java.nio.charset.Charset;
import java.util.Map;

import com.feeyo.net.codec.util.CompositeByteArray;

public abstract class HttpMessageEncoder {
	
	protected static final Charset charset = Charset.forName("UTF-8");
	protected static final byte[] CRLF = new byte[] {13, 10}; 	// <CRLF, carriage return (13) linefeed (10)>
	protected static final byte[] SP = new byte[] {32}; 		// <SP, space (32)>
	protected static final byte[] COLON = new byte[] {58}; 		// < :, colon (58)>
	
	public byte[] encode(HttpMessage message) {
		
		if (message == null)
			return null;

		CompositeByteArray buf = new CompositeByteArray();
		
		encodeHeadline(buf, message);		// headline
		encodeHeaders(buf, message);		// headers
		buf.add(CRLF);
		
		encodeContent(buf, message);		// content
		return buf.getData(0, buf.getByteCount());
		
	}
	
	protected abstract void encodeHeadline(CompositeByteArray buf, HttpMessage message);
	
	private void encodeHeaders(CompositeByteArray buf, HttpMessage message) {
		for (Map.Entry<String, String> h : message.headers().entrySet()) {
			encodeHeader(buf, h.getKey(), h.getValue());
		}
	}

	private void encodeHeader(CompositeByteArray buf, String header, String value) {
		buf.add(header.getBytes(charset));
		buf.add(COLON);
		buf.add(SP);
		buf.add(value.getBytes(charset));
		buf.add(CRLF);
	}
	
	private void encodeContent(CompositeByteArray buf, HttpMessage message) {
		
		byte[] content = message.getContent();
		if(content != null && content.length != 0)
			buf.add(content);
	}

}
