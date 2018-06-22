package com.feeyo.net.codec.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.http.handler.HttpRequestHandlerMagr;
import com.feeyo.net.codec.http.handler.RequestHandler;
import com.feeyo.net.codec.http.util.Args;
import com.feeyo.net.codec.http.util.CharArrayBuffer;
import com.feeyo.net.nio.ClosableConnection;

public class HttpDecoder {

	private static Logger LOGGER = LoggerFactory.getLogger(HttpDecoder.class);

	private static final String HTTP_1_1 = "HTTP/1.1";
	private static final String HTTP_1_0 = "HTTP/1.0";
	private static final int HTTP_HEADLINE = 0;
	private static final int HTTP_HEADERS = 1;
	private static final int HTTP_ENTITY = 2;

	private CharArrayBuffer lineCharBuf;
	private SessionInputBuffer sessionInBuf;
	private RequestHandler handler = null;
	private RequestLine requestLine = null;
	private int state;

	public HttpDecoder() {
		this.sessionInBuf = new SessionInputBuffer();
		this.lineCharBuf = new CharArrayBuffer(128);
		this.state = HTTP_HEADLINE;
	}

	public void parse(ClosableConnection conn, byte[] data) {

		if (data == null || data.length == 0)
			return;

		sessionInBuf.append(data);

		try {
			switch (state) {
			case HTTP_HEADLINE:
				requestLine = parseHeadLine();
				String uri = requestLine.getUri();
				String method = requestLine.getMethod();
				handler = HttpRequestHandlerMagr.INSTANCE().lookup(method, uri);
				this.state = HTTP_HEADERS;
			case HTTP_HEADERS:
				Args.notNull(requestLine, "Request");
				Args.notNull(handler, "HTTP REQUEST HANDLER");
				parseHeaders();
				method = requestLine.getMethod();
				switch (method) {
				case "GET":
					handler.handle(conn, requestLine.getUri(), null);
					sessionInBuf.clear();
					break;
				case "POST":
					this.state = HTTP_ENTITY;
					break;
				// TODO support other methods
				default:
					LOGGER.warn("Unsupported Method!");
				}

			case HTTP_ENTITY:
				Args.notNull(handler, "HTTP REQUEST HANDLER");
				data = sessionInBuf.readNextAll();
				handler.handle(conn, requestLine.getUri(), data);
				sessionInBuf.clear();
			default:
				break;
			}

		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			if (sessionInBuf != null) {
				LOGGER.info("data not enough or parse fail");
			}
		}

	}

	private RequestLine parseHeadLine() throws IOException {
		lineCharBuf.clear();
		int read = sessionInBuf.readLine(lineCharBuf);
		if (read == -1) {
			throw new RuntimeException("Data not enough");
		}
		return parseRequestLine(lineCharBuf, 0, lineCharBuf.length());
	}

	private void parseHeaders() throws IOException {

		List<CharArrayBuffer> headerLines = new ArrayList<CharArrayBuffer>();
		CharArrayBuffer current = null;
		CharArrayBuffer previous = null;
		for (;;) {
			if (current == null) {
				current = new CharArrayBuffer(64);
			} else {
				current.clear();
			}
			final int l = sessionInBuf.readLine(current);
			if (l == -1 || current.length() < 1) {
				break;
			}
			// Parse the header name and value
			// Check for folded headers first
			// Detect LWS-char see HTTP/1.0 or HTTP/1.1 Section 2.2
			// discussion on folded headers
			if ((current.charAt(0) == ' ' || current.charAt(0) == '\t') && previous != null) {
				// we have continuation folded header
				// so append value
				int i = 0;
				while (i < current.length()) {
					final char ch = current.charAt(i);
					if (ch != ' ' && ch != '\t') {
						break;
					}
					i++;
				}
				previous.append(' ');
				previous.append(current, i, current.length() - i);
			} else {
				headerLines.add(current);
				previous = current;
				current = null;
			}
		}

		// System.out.println(headerLines.size());
	}

	private RequestLine parseRequestLine(final CharArrayBuffer buffer, int indexFrom, int indexTo) {

		Args.notNull(buffer, "Char array buffer");

		try {
			int i = skipWhitespace(buffer, indexFrom, indexTo);

			int blank = buffer.indexOf(' ', i, indexTo);
			if (blank < 0) {
				throw new RuntimeException("Invalid request line: " + buffer.substring(indexFrom, indexTo));
			}
			final String method = buffer.substringTrimmed(i, blank);

			i = skipWhitespace(buffer, blank, indexTo);
			blank = buffer.indexOf(' ', i, indexTo);
			if (blank < 0) {
				throw new RuntimeException("Invalid request line: " + buffer.substring(indexFrom, indexTo));
			}
			final String uri = buffer.substringTrimmed(i, blank);

			i = skipWhitespace(buffer, blank, indexTo);

			final String protocolVer = buffer.substringTrimmed(i, indexTo);

			if (!(HTTP_1_1.equalsIgnoreCase(protocolVer) || HTTP_1_0.equalsIgnoreCase(protocolVer))) {
				throw new RuntimeException("Invalid request line: " + buffer.substring(indexFrom, indexTo)
						+ ", Expect http/1.1 or http/1.0");
			}

			return new RequestLine(method, uri, protocolVer);
		} catch (final IndexOutOfBoundsException e) {
			throw new RuntimeException("Invalid request line: " + buffer.substring(indexFrom, indexTo));
		}
	}

	private int skipWhitespace(final CharArrayBuffer buffer, int pos, int indexTo) {
		while ((pos < indexTo) && isWhitespace(buffer.charAt(pos))) {
			pos++;
		}
		return pos;
	}

	private boolean isWhitespace(final char ch) {
		return ch == 13 || ch == 10 || ch == 32 || ch == 9;
	}
}
