package com.feeyo.net.codec.http.util;

import java.io.Serializable;
import java.nio.CharBuffer;


public final class CharArrayBuffer implements CharSequence, Serializable {

	private static final long serialVersionUID = -6726281333893863646L;
	
	public static final int CR = 13; // <US-ASCII CR, carriage return (13)>
    public static final int LF = 10; // <US-ASCII LF, linefeed (10)>
    public static final int SP = 32; // <US-ASCII SP, space (32)>
    public static final int HT = 9;  // <US-ASCII HT, horizontal-tab (9)>

	private char[] buffer;
    private int len;

    public CharArrayBuffer(final int capacity) {
        super();
        Args.notNegative(capacity, "Buffer capacity");
        this.buffer = new char[capacity];
    }

    private void expand(final int newlen) {
        final char newbuffer[] = new char[Math.max(this.buffer.length << 1, newlen)];
        System.arraycopy(this.buffer, 0, newbuffer, 0, this.len);
        this.buffer = newbuffer;
    }

    public void append(final char[] b, final int off, final int len) {
        if (b == null) {
            return;
        }
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) < 0) || ((off + len) > b.length)) {
            throw new IndexOutOfBoundsException("off: "+off+" len: "+len+" b.length: "+b.length);
        }
        if (len == 0) {
            return;
        }
        final int newlen = this.len + len;
        if (newlen > this.buffer.length) {
            expand(newlen);
        }
        System.arraycopy(b, off, this.buffer, this.len, len);
        this.len = newlen;
    }

    public void append(final CharArrayBuffer b, final int off, final int len) {
        if (b == null) {
            return;
        }
        append(b.buffer, off, len);
    }

    public void append(final CharArrayBuffer b) {
        if (b == null) {
            return;
        }
        append(b.buffer,0, b.len);
    }

    public void append(final char ch) {
        final int newlen = this.len + 1;
        if (newlen > this.buffer.length) {
            expand(newlen);
        }
        this.buffer[this.len] = ch;
        this.len = newlen;
    }

    public void append(final byte[] b, final int off, final int len) {
        if (b == null) {
            return;
        }
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) < 0) || ((off + len) > b.length)) {
            throw new IndexOutOfBoundsException("off: "+off+" len: "+len+" b.length: "+b.length);
        }
        if (len == 0) {
            return;
        }
        final int oldlen = this.len;
        final int newlen = oldlen + len;
        if (newlen > this.buffer.length) {
            expand(newlen);
        }
        for (int i1 = off, i2 = oldlen; i2 < newlen; i1++, i2++) {
            this.buffer[i2] = (char) (b[i1] & 0xff);
        }
        this.len = newlen;
    }

    public void append(final ByteArrayBuffer b, final int off, final int len) {
        if (b == null) {
            return;
        }
        append(b.buffer(), off, len);
    }

    public void clear() {
        this.len = 0;
    }

    public char charAt(final int i) {
        return this.buffer[i];
    }

    public int capacity() {
        return this.buffer.length;
    }

    public int length() {
        return this.len;
    }

    public void ensureCapacity(final int required) {
        if (required <= 0) {
            return;
        }
        final int available = this.buffer.length - this.len;
        if (required > available) {
            expand(this.len + required);
        }
    }

    /**
     *
     * @param   ch     the char to search for.
     * @param   from   the index to start the search from.
     * @param   to     the index to finish the search at.
     * @return  the index of the first occurrence of the character in the buffer
     *   
     */
    public int indexOf(final int ch, final int from, final int to) {
        int beginIndex = from;
        if (beginIndex < 0) {
            beginIndex = 0;
        }
        int endIndex = to;
        if (endIndex > this.len) {
            endIndex = this.len;
        }
        if (beginIndex > endIndex) {
            return -1;
        }
        for (int i = beginIndex; i < endIndex; i++) {
            if (this.buffer[i] == ch) {
                return i;
            }
        }
        return -1;
    }

    public int indexOf(final int ch) {
        return indexOf(ch, 0, this.len);
    }

    public String substring(final int beginIndex, final int endIndex) {
        if (beginIndex < 0) {
            throw new IndexOutOfBoundsException("Negative beginIndex: " + beginIndex);
        }
        if (endIndex > this.len) {
            throw new IndexOutOfBoundsException("endIndex: " + endIndex + " > length: " + this.len);
        }
        if (beginIndex > endIndex) {
            throw new IndexOutOfBoundsException("beginIndex: " + beginIndex + " > endIndex: " + endIndex);
        }
        return new String(this.buffer, beginIndex, endIndex - beginIndex);
    }

    public String substringTrimmed(final int beginIndex, final int endIndex) {
        if (beginIndex < 0) {
            throw new IndexOutOfBoundsException("Negative beginIndex: " + beginIndex);
        }
        if (endIndex > this.len) {
            throw new IndexOutOfBoundsException("endIndex: " + endIndex + " > length: " + this.len);
        }
        if (beginIndex > endIndex) {
            throw new IndexOutOfBoundsException("beginIndex: " + beginIndex + " > endIndex: " + endIndex);
        }
        int beginIndex0 = beginIndex;
        int endIndex0 = endIndex;
        while (beginIndex0 < endIndex && isWhitespace(this.buffer[beginIndex0])) {
            beginIndex0++;
        }
        while (endIndex0 > beginIndex0 && isWhitespace(this.buffer[endIndex0 - 1])) {
            endIndex0--;
        }
        return new String(this.buffer, beginIndex0, endIndex0 - beginIndex0);
    }

    private boolean isWhitespace(char ch) {
    	   return ch == SP || ch == HT || ch == CR || ch == LF;
	}

    @Override
    public CharSequence subSequence(final int beginIndex, final int endIndex) {
        if (beginIndex < 0) {
            throw new IndexOutOfBoundsException("Negative beginIndex: " + beginIndex);
        }
        if (endIndex > this.len) {
            throw new IndexOutOfBoundsException("endIndex: " + endIndex + " > length: " + this.len);
        }
        if (beginIndex > endIndex) {
            throw new IndexOutOfBoundsException("beginIndex: " + beginIndex + " > endIndex: " + endIndex);
        }
        return CharBuffer.wrap(this.buffer, beginIndex, endIndex);
    }

    @Override
    public String toString() {
        return new String(this.buffer, 0, this.len);
    }

}
