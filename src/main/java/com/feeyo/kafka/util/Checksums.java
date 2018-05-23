package com.feeyo.kafka.util;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

/**
 * Utility methods for `Checksum` instances.
 *
 * Implementation note: we can add methods to our implementations of CRC32 and CRC32C, but we cannot do the same for
 * the Java implementations (we prefer the Java 9 implementation of CRC32C if available). A utility class is the
 * simplest way to add methods that are useful for all Checksum implementations.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
public final class Checksums {

    private Checksums() {
    }

    public static void update(Checksum checksum, ByteBuffer buffer, int length) {
        update(checksum, buffer, 0, length);
    }

    public static void update(Checksum checksum, ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray()) {
            checksum.update(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, length);
        } else {
            int start = buffer.position() + offset;
            for (int i = start; i < start + length; i++)
                checksum.update(buffer.get(i));
        }
    }
    
    public static void updateInt(Checksum checksum, int input) {
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        checksum.update((byte) input /* >> 0 */);
    }

    public static void updateLong(Checksum checksum, long input) {
        checksum.update((byte) (input >> 56));
        checksum.update((byte) (input >> 48));
        checksum.update((byte) (input >> 40));
        checksum.update((byte) (input >> 32));
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        checksum.update((byte) input /* >> 0 */);
    }
}
