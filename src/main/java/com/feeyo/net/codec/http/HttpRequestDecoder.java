package com.feeyo.net.codec.http;

import java.nio.charset.Charset;

import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.util.CompositeByteArray;
import com.feeyo.net.codec.util.CompositeByteArray.ByteArrayChunk;

/**
 * Http request parse
 * 
 * @author xuwenfeng
 *
 */
public class HttpRequestDecoder implements Decoder<HttpRequest> {

	private static final Charset charset = Charset.forName("UTF-8");
	
	private static final byte CR = 13; 			// <CR, carriage return (13)>
	private static final byte LF = 10; 			// <LF, linefeed (10)>
	private static final char SLASH = '/';
    private static final char QUESTION_MARK = '?';

	private enum State {
        SKIP_CONTROL_CHARS,
        READ_INITIAL,
        READ_HEADER,
        READ_VARIABLE_LENGTH_CONTENT,
        READ_FIXED_LENGTH_CONTENT,
        READ_CHUNK_SIZE,
        READ_CHUNKED_CONTENT,
        READ_CHUNK_DELIMITER,
        READ_CHUNK_FOOTER
	}
	
	private int contentSize = 0;
	
	private CompositeByteArray compositeArray = null;
	private CompositeByteArray chunkContent = null;
	private ByteArrayChunk readChunk = null;
	private int _offset = 0;

	private State state;
	private HttpRequest request;

	public HttpRequestDecoder() {
		this.state = State.SKIP_CONTROL_CHARS;
	}

	public HttpRequest decode(byte[] data) {

		if (data == null || data.length == 0)
			return null;

		append(data);
		readChunk = compositeArray.findChunk(_offset);
		
		for(;;) {
			
			switch (state) {

			case SKIP_CONTROL_CHARS:{
				skipControlCharacters();
				this.state = State.READ_INITIAL;
			}
			
			case READ_INITIAL:{
				parseHeadLine();
				this.state = State.READ_HEADER;
			}
			
			case READ_HEADER:{

				if (this.request == null) {
					throw new IllegalArgumentException("Http request may be not null");
				}
				
				parseHeaders();
				continue;
			}
			
			case READ_FIXED_LENGTH_CONTENT:{
				
				if (request == null) {
					throw new IllegalArgumentException("Http request may be not null");
				}
				
				if(contentSize <= 0 ) {
					throw new IllegalArgumentException("Http content may be not null");
				}

				int len = compositeArray.getByteCount() - _offset;
				if(contentSize <= len) {
					data = compositeArray.getData(_offset, contentSize);
					request.setContent(data);
					clear();
					return request;
					
				}else {
					return null; 	// data not enough;
				}
			}
				
			case READ_VARIABLE_LENGTH_CONTENT:{
				
				if (request == null) {
					throw new IllegalArgumentException("Http request may be not null");
				}
				int contentLen = compositeArray.getByteCount() - _offset;
				if(contentLen > 0) {
					data = compositeArray.getData(_offset, contentLen);
					request.setContent(data);
				}
				clear();
				return request;
			}
				
			case READ_CHUNK_SIZE:{
				
				if(chunkContent == null) {
					chunkContent = new CompositeByteArray();
				}
				
				byte[] chunkSizeHex = readLine();
				if(chunkSizeHex == null) {
					return null;
				}
				
				contentSize = Integer.parseInt(new String(chunkSizeHex, charset).trim(), 16);
		        state = contentSize == 0 ? State.READ_CHUNK_FOOTER : State.READ_CHUNKED_CONTENT;
		        continue;
			}
			
			case READ_CHUNKED_CONTENT:{
				
				byte[] chunk = read(contentSize);
				if(chunk == null) {
					return null;
				}
				
				chunkContent.add(chunk);
				
	            state = State.READ_CHUNK_DELIMITER;
	            continue;
			}
			
			case READ_CHUNK_DELIMITER:{
				//delimiter - CRLF
				byte[] delimiter = readLine();
				if(delimiter == null) {
					return null;
				}
				
				state = State.READ_CHUNK_SIZE;
				continue;
			}
				
			case READ_CHUNK_FOOTER:{
				request.setContent(chunkContent.getData(0, chunkContent.getByteCount()));
				clear();
				return request;
			}
			default:
				clear();
				return request;
			}
		}
	}

	private void skipControlCharacters() {
		
        while (compositeArray.getByteCount() > _offset) {
        	
            int c = readChunk.get(_offset++) & 0xFF;
            if (!Character.isISOControl(c) && !Character.isWhitespace(c)) {
            	_offset --;
                break;
            }
            updateReadOffsetAndReadByteChunk(_offset);
        }
	}
	
	// 在遍历中改变readOffset可能需要更新 readByteChunk
    private void updateReadOffsetAndReadByteChunk(int newReadOffset) {
    	
        while (readChunk != null) {
            // 当offset达到最大长度时也不继续,防止空指针异常
            if (readChunk.isInBoundary(newReadOffset) || newReadOffset == compositeArray.getByteCount() ) {
                return;
            }
            readChunk = readChunk.getNext();
        }
    }

	private void append(byte[] newBuffer) {

		if (newBuffer == null) {
            return;
        }

        if (compositeArray == null) {
            compositeArray = new CompositeByteArray();
        }

        // large packet
        compositeArray.add(newBuffer);
	}

	private byte[] readLine() {
		int pos = -1;
		byte[] _linebuf = null;
		for (int i = _offset; i < compositeArray.getByteCount(); i++) {
			updateReadOffsetAndReadByteChunk(i);
			if (readChunk.get(i) == LF) {
				pos = i;
				break;
			}
		}

		if (pos != -1) {
			// end of line found
			final int len = pos + 1 - _offset;
			_linebuf = compositeArray.getData(_offset, len);
			_offset = pos + 1;
		}
		updateReadOffsetAndReadByteChunk(_offset);
		return _linebuf;
	}
	
	private byte[] read(int len) {
		
		if(len <= 0 || compositeArray.getByteCount() - _offset < len)
			return null;
		byte[] ret = compositeArray.getData(_offset, len);
		_offset += len;
		return ret;
	}

	private void parseHeadLine() {

		byte[] linebuff = readLine();
		if (linebuff == null) {
			throw new RuntimeException("Data not enough");
		}
		String headLine = new String(linebuff, charset).trim();
		String[] parts = headLine.split(" ", 3);
		
		if (parts == null || parts.length != 3) {
			throw new RuntimeException("Invalid request line: " + headLine);
		}
		
		String method = parts[0];
		String uri = parts[1];
		int start = uri.indexOf("://");
        if (start != -1) {
            int startIndex = uri.indexOf(SLASH, start + 3);
            if(startIndex == -1) {
            	throw new RuntimeException("Http uri may out of form ");
            }else {
            	int index = uri.indexOf(QUESTION_MARK, startIndex);
            	uri = index == -1 ? uri.substring(startIndex) : uri.substring(startIndex, index);
            }
        }
		
		String protocol = parts[2];
		request =  new HttpRequest(protocol, method, uri);
		
	}

	private void parseHeaders() {

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
				request.addHeader(headerName, value);
			}
		}

		if (request.containsHeader("transfer-encoding", "chunked", true)) {
			state = State.READ_CHUNK_SIZE;
		} else {
		
			String value = request.headers().get("content-length");
			contentSize = value != null ? Integer.parseInt(value) : -1;
			if (contentSize >= 0) {
				state = State.READ_FIXED_LENGTH_CONTENT;
			} else {
				state = State.READ_VARIABLE_LENGTH_CONTENT;
			}
		}
	}
	
	private void clear() {
		
		if(compositeArray!= null) {
			compositeArray.clear();
		}
		
		if(chunkContent != null) {
			chunkContent.clear();
		}
		
		_offset = 0;
	}

}
