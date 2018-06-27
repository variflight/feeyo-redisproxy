package com.feeyo.net.codec.http;

import java.io.IOException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.UnknowProtocolException;

public class HttpDecoder implements Decoder<HttpRequest> {

	private static Logger LOGGER = LoggerFactory.getLogger(HttpDecoder.class);

	private static final int CR = 13; // <US-ASCII CR, carriage return (13)>
    private static final int LF = 10; // <US-ASCII LF, linefeed (10)>	
	
	private static final int HTTP_REQ_HEADLINE = 0;
	private static final int HTTP_REQ_HEADERS = 1;
	private static final int HTTP_REQ_BODY = 2;

    private String method;
    private String uri;
	
	private byte[] _buffer = null;
	private int _offset = 0;
	
	private int state;
	
	//
    //
	private HttpRequest httpRequest = null;
	

	public HttpDecoder() {
		this.state = HTTP_REQ_HEADLINE;
	}

	
	// parse POST & GET 
	//
	//
	public HttpRequest decode(byte[] data) throws UnknowProtocolException {

		if (data == null || data.length == 0)
			return null;

		append(data);
		
		try {
			switch (state) {
			case HTTP_REQ_HEADLINE:
				
				parseHeadLine();
				if (uri == null || method == null) {
		            throw new IllegalArgumentException("Http uri or method may not be null");
		        }
				
				this.httpRequest = new HttpRequest(uri, method);
				this.state = HTTP_REQ_HEADERS;
				
			case HTTP_REQ_HEADERS:
				
				if (this.httpRequest == null) {
		            throw new IllegalArgumentException("Http request may not be null");
		        }
				
				parseHeaders();
				switch (method.toUpperCase()) {
				case "GET":
					clear();
					return httpRequest;
				case "POST":							
					this.state = HTTP_REQ_BODY;
					break;
				default:
					LOGGER.warn("Unsupported Method!");
					break;
				}

			case HTTP_REQ_BODY:
				if (httpRequest == null) {
		            throw new IllegalArgumentException("Http request may not be null");
		        }
				
				int len = _buffer.length - _offset;
				if(_buffer == null || len == 0)
					return null;
				
				//request body
				data = new byte[len];
				System.arraycopy(_buffer, _offset, data, 0, len);
				clear();
				
				this.httpRequest.setData(data);	// handle post req
				return httpRequest;
			default:
				break;
			}

		} catch (IOException e) {
			LOGGER.error(e.getMessage());
			clear();
		}
		
		return null;

	}
	
	private void append(byte[] b) {
		
		if (b == null)
			return;
		
	    if (_buffer == null) {
	      _buffer = b;
	      return;
	    }
	    
    	byte[] largeBuffer = new byte[ _buffer.length + b.length ];
    	System.arraycopy(_buffer, 0, largeBuffer, 0, _buffer.length);
    	System.arraycopy(b, 0, largeBuffer, _buffer.length, b.length);
    	
    	_buffer = largeBuffer;
	}

	private byte[] readLine() {
		int pos = -1;
		byte[] _linebuf = null;
		for(int i = _offset; i < _buffer.length; i++) {
			if(_buffer[i] == LF) {
				pos = i;
				break;
			}
		}
		
		if (pos != -1) {
			//end of line found
			final int len = pos + 1 - _offset;
			_linebuf = new byte[len];
			System.arraycopy(_buffer, _offset, _linebuf, 0, len);
			_offset = pos + 1;
			return _linebuf;
		} else {
			// end of line not found, data not enough
			return null;
		}
	}
	
	private void parseHeadLine() throws IOException {
		
		byte[] linebuff = readLine();
		if (linebuff == null) {	// data not enough
			throw new RuntimeException("Data not enough");
		}
		String headLine = new String(linebuff, "UTF-8").trim();
		String[] parts = headLine.split(" ");
		if(parts == null || parts.length != 3) {
			throw new RuntimeException("Invalid request line: " + headLine);
		}
		method = parts[0].trim();
		uri = parts[1].trim();
	}
	
	private void parseHeaders() throws IOException {

		HashMap<String, String> headers = new HashMap<String, String>();
		for (;;) {
			
			final byte[] linebuf = readLine();
			if (linebuf == null) {
				break;
			}
			
			if(linebuf[0] == LF || (linebuf[0] == CR && linebuf[1] == LF)) {
				break;
			}
			// 
			String line = new String(linebuf,"UTF-8").trim(); 
			int k = line.indexOf(":");
			if (k != -1) {
				String headerName = line.substring(0, k).trim().toLowerCase();
				String value = line.substring(k + 1).trim();
				headers.put(headerName, value);
			}
		}
	}

	public void clear() {
		_buffer = null;
		_offset = 0;
	}
	

}