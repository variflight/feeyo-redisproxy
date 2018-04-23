package com.feeyo.redis.kafka.util;

import java.nio.ByteBuffer;

public class ByteUtils {

	/**
     * Read an unsigned integer from the current position in the buffer, incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }
    
    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }
    
    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }
    
    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeVarint(int value, ByteBuffer buffer) {
        int v = (value << 1) ^ (value >> 31);
        while ((v & 0xffffff80) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.put(b);
            v >>>= 7;
        }
        buffer.put((byte) v);
    }
    
    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readVarint(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw illegalVarintException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }
    
    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeVarlong(long value, ByteBuffer buffer) {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.put(b);
            v >>>= 7;
        }
        buffer.put((byte) v);
    }
    
    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     */
    public static long readVarlong(ByteBuffer buffer)  {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }
    
    /**
     * Number of bytes needed to encode a long in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarlong(long value) {
        long v = (value << 1) ^ (value >> 63);
        int bytes = 1;
        while ((v & 0xffffffffffffff80L) != 0L) {
            bytes += 1;
            v >>>= 7;
        }
        return bytes;
    }
    
    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarint(int value) {
        int v = (value << 1) ^ (value >> 31);
        int bytes = 1;
        while ((v & 0xffffff80) != 0L) {
            bytes += 1;
            v >>>= 7;
        }
        return bytes;
    }
    
    private static IllegalArgumentException illegalVarintException(int value) {
    	throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, " +
    			"converted value: " + Integer.toHexString(value));
    }
    
    private static IllegalArgumentException illegalVarlongException(long value) {
        throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, " +
                "converted value: " + Long.toHexString(value));
    }
}
