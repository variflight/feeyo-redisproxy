package com.feeyo.net.codec.http;

import java.nio.charset.Charset;


public abstract class HttpMessageDecoder<T extends HttpMessage> {

	private static final Charset charset = Charset.forName("UTF-8");
	
	private static final byte CR = 13; 			// <CR, carriage return (13)>
	private static final byte LF = 10; 			// <LF, linefeed (10)>

	private enum State {
        SKIP_CONTROL_CHARS,
        READ_INITIAL,
        READ_HEADER,
        READ_VARIABLE_LENGTH_CONTENT,
        READ_FIXED_LENGTH_CONTENT,
        READ_CHUNK_SIZE,
	}

	private final boolean chunkedSupported;
	private int contentSize = 0;
	
	private byte[] _buffer = null;
	private int _offset = 0;

	private State state;
	private T message;

	public HttpMessageDecoder() {
		this.state = State.SKIP_CONTROL_CHARS;
		this.chunkedSupported = false;
	}

	protected abstract T createMessage(String[] headline);
	protected abstract boolean isDecodingRequest();

	public T decode(byte[] data) {

		if (data == null || data.length == 0)
			return null;

		append(data);

		for(;;) {
			switch (state) {

			case SKIP_CONTROL_CHARS:
				skipControlCharacters();
				this.state = State.READ_INITIAL;
			
			case READ_INITIAL:
				String[] headline = parseHeadLine();
				message = createMessage(headline);
				this.state = State.READ_HEADER;

			case READ_HEADER:

				if (this.message == null) {
					throw new IllegalArgumentException("Http message may be not null");
				}
				
				State nextState = parseHeaders();
				
				if(nextState == null)
					return message;
				this.state = nextState;
				
				switch(nextState) {
				case SKIP_CONTROL_CHARS:
					return message;
				case READ_CHUNK_SIZE:
					if (!chunkedSupported) {
		                throw new IllegalArgumentException("Chunked messages not supported");
		            }
					return message;
				default:
					int contentLength = contentLength();
					if (contentLength == 0 || contentLength == -1 && isDecodingRequest()) {
		                continue;
		            }
					
					assert nextState == State.READ_FIXED_LENGTH_CONTENT ||
		                  nextState == State.READ_VARIABLE_LENGTH_CONTENT;
					
					if(nextState == State.READ_FIXED_LENGTH_CONTENT) {
						contentSize = contentLength;
					}
					continue;
				}
				
			case READ_FIXED_LENGTH_CONTENT:
				
				if (message == null) {
					throw new IllegalArgumentException("Http message may be not null");
				}
				
				if(contentSize <= 0 ) {
					throw new IllegalArgumentException("Http content may be not null");
				}

				int len = _buffer.length - _offset;
				if(contentSize <= len) {
					data = new byte[contentSize];
					System.arraycopy(_buffer, _offset, data, 0, contentSize);
					message.setContent(data);
					clear();
					return message;
				}else {
					return null; 	// data not enough;
				}
				
			case READ_VARIABLE_LENGTH_CONTENT:
				
				if (message == null) {
					throw new IllegalArgumentException("Http message may be not null");
				}
				len = _buffer.length - _offset;
				if(len > 0) {
					data = new byte[len];
					System.arraycopy(_buffer, _offset, data, 0, len);
					message.setContent(data);
				}
				clear();
				return message;
			default:
				clear();
				return message;
			}
		}
	}

	private void skipControlCharacters() {
        while (_buffer.length > _offset) {
            int c = _buffer[_offset++] & 0xFF;
            if (!Character.isISOControl(c) && !Character.isWhitespace(c)) {
            	_offset --;
                break;
            }
        }
	}

	private void append(byte[] b) {

		if (b == null)
			return;

		if (_buffer == null) {
			_buffer = b;
			return;
		}

		byte[] largeBuffer = new byte[_buffer.length + b.length];
		System.arraycopy(_buffer, 0, largeBuffer, 0, _buffer.length);
		System.arraycopy(b, 0, largeBuffer, _buffer.length, b.length);

		_buffer = largeBuffer;
	}

	private byte[] readLine() {
		int pos = -1;
		byte[] _linebuf = null;
		for (int i = _offset; i < _buffer.length; i++) {
			if (_buffer[i] == LF) {
				pos = i;
				break;
			}
		}

		if (pos != -1) {
			// end of line found
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

	private String[] parseHeadLine() {

		byte[] linebuff = readLine();
		if (linebuff == null) {
			throw new RuntimeException("Data not enough");
		}
		String headLine = new String(linebuff, charset).trim();
		String[] parts = headLine.split(" ", 3);
		
		if (parts == null || parts.length != 3) {
			throw new RuntimeException("Invalid request line: " + headLine);
		}
		return parts;
	}

	private State parseHeaders() {

		for (;;) {

			final byte[] linebuf = readLine();
			if (linebuf == null) {
				break;
			}

			if (linebuf[0] == LF || (linebuf[0] == CR && linebuf[1] == LF)) {
				break;
			}
			//
			String line = new String(linebuf, charset).trim();
			int k = line.indexOf(":");
			if (k != -1) {
				String headerName = line.substring(0, k).trim().toLowerCase();
				String value = line.substring(k + 1).trim();
				message.addHeader(headerName, value);
			}
		}

		State nextState = null;
		if (isContentAlwaysEmpty(message)) {
            nextState = State.SKIP_CONTROL_CHARS;
        } else if (isTransferEncodingChunked(message)) {
			nextState = State.READ_CHUNK_SIZE;
		} else if (contentLength() >= 0) {
			nextState = State.READ_FIXED_LENGTH_CONTENT;
		} else {
			nextState = State.READ_VARIABLE_LENGTH_CONTENT;
		}
		return nextState;
	}

	private boolean isContentAlwaysEmpty(T msg) {
		if (msg instanceof HttpResponse) {
            HttpResponse res = (HttpResponse) msg;
            int code = res.getStatusCode();

            if (code >= 100 && code < 200) {
                return !(code == 101 && !res.containsHeader("sec-websocket-accept")
                         && res.containsHeader("upgrade", "websocket", true));
            }

            switch (code) { 
            case 204: case 304:
                return true;
            }
        }
        return false;
	}

	private int contentLength() {
		String value = message.headers().get("content-length");
		if (value != null) {
			return Integer.parseInt(value);
		}

		// We know the content length if it's a Web Socket message even if
		// Content-Length header is missing.
		int webSocketContentLength = getWebSocketContentLength(message);
		if (webSocketContentLength >= 0) {
			return webSocketContentLength;
		}

		// Otherwise we don't.
		return -1;
	}

	private int getWebSocketContentLength(T msg) {
		
		if (msg instanceof HttpRequest) {
			HttpRequest req = (HttpRequest) msg;
			if ("GET".equalsIgnoreCase(req.getMethod()) && msg.containsHeader("sec-websocket-key1")
					&& msg.containsHeader("sec-websocket-key2")) {
				return 8;
			}
		} else if (msg instanceof HttpResponse) {
			HttpResponse res = (HttpResponse) msg;
			if (res.getStatusCode() == 101 && msg.containsHeader("sec-websocket-origin")
					&& msg.containsHeader("sec-websocket-location")) {
				return 16;
			}
		}
		return -1;
	}

	private boolean isTransferEncodingChunked(T msg) {
		return msg.containsHeader("transfer-encoding", "chunked", true);
	}

	private void clear() {
		_buffer = null;
		_offset = 0;
	}

}
