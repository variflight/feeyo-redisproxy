package com.feeyo.net.codec.http.util;

import java.io.Serializable;


public final class ByteArrayBuffer implements Serializable{

	private static final long serialVersionUID = -6253318199835521329L;
	private byte[] buffer;
    private int len;

    public ByteArrayBuffer(final int capacity) {
        super();
        Args.notNegative(capacity, "Buffer capacity");
        this.buffer = new byte[capacity];
    }

    private void expand(final int newlen) {
        final byte newbuffer[] = new byte[Math.max(this.buffer.length << 1, newlen)];
        System.arraycopy(this.buffer, 0, newbuffer, 0, this.len);
        this.buffer = newbuffer;
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
        final int newlen = this.len + len;
        if (newlen > this.buffer.length) {
            expand(newlen);
        }
        System.arraycopy(b, off, this.buffer, this.len, len);
        this.len = newlen;
    }

    public void clear() {
        this.len = 0;
    }

    public byte[] toByteArray() {
        final byte[] b = new byte[this.len];
        if (this.len > 0) {
            System.arraycopy(this.buffer, 0, b, 0, this.len);
        }
        return b;
    }

    public int byteAt(final int i) {
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

    public byte[] buffer() {
        return this.buffer;
    }

    public boolean isEmpty() {
        return this.len == 0;
    }

}
