package com.feeyo.net.codec.http;

import java.io.IOException;

import com.feeyo.net.codec.http.util.ByteArrayBuffer;
import com.feeyo.net.codec.http.util.CharArrayBuffer;

public class SessionInputBuffer {
	
	public static final int CR = 13; // <US-ASCII CR, carriage return (13)>
    public static final int LF = 10; // <US-ASCII LF, linefeed (10)>
    
	private final ByteArrayBuffer lineByteBuf;
	
	private byte[] _buffer = null;
	private int _offset = 0;
	
	public SessionInputBuffer() {
		this.lineByteBuf = new ByteArrayBuffer(128);
	}
	
	public byte[] readNextAll() throws IOException {
		if(_buffer == null)
			return null;
		byte[] buf = new byte[_buffer.length - _offset];
		System.arraycopy(_buffer, _offset, buf, 0, buf.length);
		// unset _offset = _buffer.length in case of data no enough
		return buf;
	}
	
	public int append(byte[] b) {
		
		if (b == null) {
			return 0;
		}
		
	    if (_buffer == null) {
	      _buffer = b;
	      return b.length;
	    }
	    
	    // large packet	    	
    	byte[] largeBuffer = new byte[ _buffer.length + b.length ];
    	System.arraycopy(_buffer, 0, largeBuffer, 0, _buffer.length);
    	System.arraycopy(b, 0, largeBuffer, _buffer.length, b.length);
    	
    	_buffer = largeBuffer;
	    return b.length;
	}

	public int readLine(CharArrayBuffer buffer) throws IOException {
		if (buffer == null) {
	        throw new IllegalArgumentException("char array buf may not be null");
	    }
		
		int pos = -1;
		for(int i = _offset; i < _buffer.length; i++) {
			if(_buffer[i] == LF) {
				pos = i;
				break;
			}
		}
		
		if (pos != -1) {
			//end of line found
			if (this.lineByteBuf.isEmpty()) {
				// the entire line is preset in the read buffer
				return lineFromReadBuffer(buffer, pos);
			}
			final int len = pos + 1 - _offset;
			lineByteBuf.append(_buffer, _offset, len);
			_offset = pos + 1;
		} else {
			// end of line not found, push into byteBuf
			if(_offset < _buffer.length) {
				final int len = _buffer.length - _offset;
				lineByteBuf.append(_buffer, _offset, len);
				_offset = _buffer.length;
			}
			// data not enough
			return -1;
		}
		
		if(this.lineByteBuf.isEmpty())
			return -1;
		
		return lineFromLineBuffer(buffer);
	}

	private int lineFromLineBuffer(final CharArrayBuffer charbuffer)
            throws IOException {
        // discard LF if found
        int len = this.lineByteBuf.length();
        if (len > 0) {
            if (this.lineByteBuf.byteAt(len - 1) == LF) {
                len--;
            }
            // discard CR if found
            if (len > 0) {
                if (this.lineByteBuf.byteAt(len - 1) == CR) {
                    len--;
                }
            }
        }
            charbuffer.append(this.lineByteBuf, 0, len);
        this.lineByteBuf.clear();
        return len;
    }

	private int lineFromReadBuffer(final CharArrayBuffer charbuffer, final int position)
            throws IOException {
        int pos = position;
        final int off = this._offset;
        int len;
        this._offset = pos + 1;
        if (pos > off && this._buffer[pos - 1] == CR) {
            // skip CR if found
            pos--;
        }
        len = pos - off;
            charbuffer.append(this._buffer, off, len);
        return len;
    }
	
	public void clear() {
		_buffer = null;
		_offset = 0;
		lineByteBuf.clear();
	}
	
}
