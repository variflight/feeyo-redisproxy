package com.feeyo.redis.kafka.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class Utils {
    private static AtomicInteger index = new AtomicInteger(0);
	
    /**
     * Turn a string into a utf8 byte[]
     *
     * @param string The string
     * @return The byte[]
     */
    public static byte[] utf8(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Read a UTF8 string from a byte buffer. Note that the position of the byte buffer is not affected
     * by this method.
     *
     * @param buffer The buffer to read from
     * @param length The length of the string in bytes
     * @return The UTF8 string
     */
    public static String utf8(ByteBuffer buffer, int length) {
        return utf8(buffer, 0, length);
    }
    
    /**
     * Read a UTF8 string from a byte buffer at a given offset. Note that the position of the byte buffer
     * is not affected by this method.
     *
     * @param buffer The buffer to read from
     * @param offset The offset relative to the current position in the buffer
     * @param length The length of the string in bytes
     * @return The UTF8 string
     */
    public static String utf8(ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray())
            return new String(buffer.array(), buffer.arrayOffset() + buffer.position() + offset, length, StandardCharsets.UTF_8);
        else
            return utf8(toArray(buffer, offset, length));
    }
    
    /**
     * Read a byte array from the given offset and size in the buffer
     * @param buffer The buffer to read from
     * @param offset The offset relative to the current position of the buffer
     * @param size The number of bytes to read into the array
     */
    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            int pos = buffer.position();
            buffer.position(pos + offset);
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    /**
     * Turn the given UTF8 byte array into a string
     *
     * @param bytes The byte array
     * @return The string
     */
    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
    /**
     * Get the length for UTF8-encoding a string without encoding it first
     *
     * @param s The string to calculate the length for
     * @return The length when serialized
     */
    public static int utf8Length(CharSequence s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }
    
	public static int getCorrelationId() {
		for (;;) {
			int current = index.get();
			int next = current + 1;
			if (next < 0)
				next = 0;
			if (index.compareAndSet(current, next))
				return current;
		}
	}
    
}
