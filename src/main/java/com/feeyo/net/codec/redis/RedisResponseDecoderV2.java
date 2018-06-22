package com.feeyo.net.codec.redis;

import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.util.CompositeByteArray;

import java.util.ArrayList;
import java.util.List;

/**
 * 返回  type+data+\r\n 字节流， 避免 encode <br>
 * 在 RedisResponseDecoder 实现上使用 CompositeByteArray 代替 byte[]
 *
 * @see com.feeyo.net.codec.redis.RedisResponseDecoder
 * @see "https://redis.io/topics/protocol"
 */
public class RedisResponseDecoderV2 implements Decoder<List<RedisResponse>> {
    private CompositeByteArray byteArray;
    // 用于标记读取的位置
    private int readOffset;
    private List<RedisResponse> responses = null;
    private long findCost = 0;
    private long readCost = 0;

    @Override
    public List<RedisResponse> decode(byte[] buffer) {

        append(buffer);

        try {
            if (responses != null) {
                responses.clear();
            } else {
                responses = new ArrayList<>(2);
            }

            int length = byteArray.getByteCount();
            for (; ; ) {
                // 至少4字节  :1\r\n
                if (byteArray.remaining(readOffset) < 4) {
                    return null;
                }

                byte type = byteArray.get(readOffset++);
                switch (type) {
                    case '*':   // 多条批量回复(multi bulk reply)的第一个字节是 "*", 后面紧跟着的长度表示多条回复的数量
                    case '+':   // 状态回复(status reply)的第一个字节是 "+"
                    case '-':   // 错误回复(error reply)的第一个字节是 "-"
                    case ':':   // 整数回复(integer reply)的第一个字节是 ":"
                    case '$':   // 批量回复(bulk reply)的第一个字节是 "$", 后面紧跟着的回复长度的数字值, 字符串最大长度为512MB
                        RedisResponse resp = parseResponse(type);
                        if (resp != null) {
                            responses.add(resp);
                        }
                }

                if (length < readOffset) {
                    throw new IndexOutOfBoundsException("Not enough data.");
                } else if (length == readOffset) {
                    byteArray.clear();
                    return responses;
                }
            }

        } catch (IndexOutOfBoundsException e1) {
            // 捕获这个错误（没有足够的数据），等待下一个数据包
            readOffset = 0;
            return null;
        }
    }

    private RedisResponse parseResponse(byte type) {
        int start, end, len, offset;
        int length = byteArray.getByteCount();

        if (type == '+' || type == '-' || type == ':') {
            offset = readOffset;
            start = offset - 1;
            end = readCRLFOffset();    // 分隔符 \r\n
            readOffset = end;   // 调整偏移值

            if (end > length) {
                readOffset = offset;
                throw new IndexOutOfBoundsException("Wait for more data.");
            }

            // 长度
            len = end - start;
            return new RedisResponse(type, byteArray.subArray(start, len));
        } else if (type == '$') {
            offset = readOffset;

            // 大小为 -1的数据包被认为是 NULL
            int packetSize = readInt();
            if (packetSize == -1) {
                start = offset - 1;
                end = readOffset;
                len = end - start;
                return new RedisResponse(type, byteArray.subArray(start, len));  // 此处不减
            }

            end = readOffset + packetSize + 2;    // offset + data + \r\n
            readOffset = end;   // 调整偏移值

            if (end > length) {
                readOffset = offset - 1;
                throw new IndexOutOfBoundsException("Wait for more data.");
            }

            start = offset - 1;
            len = end - start;
            return new RedisResponse(type, byteArray.subArray(start, len));
        } else if (type == '*') {
            offset = readOffset;

            // 大小为 -1的数据包被认为是 NULL
            int packetSize = readInt();
            if (packetSize == -1) {
                start = offset - 1;
                end = readOffset;
                len = end - start;
                return new RedisResponse(type, byteArray.subArray(start, len));  // 此处不减
            }

            if (packetSize > byteArray.remaining(readOffset)) {
                readOffset = offset - 1;
                throw new IndexOutOfBoundsException("Wait for more data.");
            }

            // 此处多增加一长度，用于存储 *packetSize\r\n
            RedisResponse response = new RedisResponse(type, packetSize + 1);
            start = offset - 1;
            end = readOffset;
            len = end - start;
            response.set(0, new RedisResponse(type, byteArray.subArray(start, len)));

            byte nType;
            RedisResponse res;
            for (int i = 1; i <= packetSize; i++) {
                if (readOffset + 1 >= length) {
                    throw new IndexOutOfBoundsException("Wait for more data.");
                }

                nType = byteArray.get(readOffset++);
                res = parseResponse(nType);
                response.set(i, res);
            }
            return response;
        }

        return null;
    }

    private int readInt() throws IndexOutOfBoundsException {
        long t1 = System.currentTimeMillis();
        int length = byteArray.getByteCount();
        long size = 0;
        boolean isNeg = false;

        if (readOffset >= length) {
            throw new IndexOutOfBoundsException("Not enough data.");
        }

        byte b = byteArray.get(readOffset);
        while (b != '\r') {
            if (b == '-') {
                isNeg = true;
            } else {
                size = size * 10 + b - '0';
            }
            readOffset++;

            if (readOffset >= length) {
                throw new IndexOutOfBoundsException("Not enough data.");
            }
            b = byteArray.get(readOffset);
        }

        // skip \r\n
        readOffset++;
        readOffset++;

        size = (isNeg ? -size : size);
        if (size > Integer.MAX_VALUE) {
            throw new RuntimeException("Cannot allocate more than " + Integer.MAX_VALUE + " bytes");
        }
        if (size < Integer.MIN_VALUE) {
            throw new RuntimeException("Cannot allocate less than " + Integer.MIN_VALUE + " bytes");
        }

        readCost += (System.currentTimeMillis() - t1);
        return (int) size;
    }

    /**
     * 往前查找第一个 '\r\n' 对应的offset值
     *
     * @return '\r\n' 对应的offset值
     * @throws IndexOutOfBoundsException
     */
    private int readCRLFOffset() throws IndexOutOfBoundsException {
        long t1 = System.currentTimeMillis();
        int offset = readOffset;
        int length = byteArray.getByteCount();

        if (offset + 1 >= length) {
            throw new IndexOutOfBoundsException("Not enough data.");
        }

        while (byteArray.get(offset) != '\r' && byteArray.get(offset + 1) != '\n') {
            offset++;
            if (offset + 1 == length) {
                throw new IndexOutOfBoundsException("didn't see LF after NL reading multi bulk count (" + offset + " => " + length +
                        ", " + offset + ")");
            }
        }
        offset++;
        offset++;
        findCost += (System.currentTimeMillis() - t1);
        return offset;
    }

    private void append(byte[] newBuffer) {
        if (newBuffer == null) {
            return;
        }

        if (byteArray == null) {
            byteArray = new CompositeByteArray();
        }

        // large packet
        byteArray.add(newBuffer);
        readOffset = 0;
    }

    /**
     * 性能测试结果： <br>
     * TODO 性能反而不如V1版本???
     * 响应包长度为538 <br>
     * 循环1kw次解析半包耗时约54s, V1版本约为38s(与包的数量有关) <br>
     * 循环1kw次解析整包耗时约27s, V1版本约为19s <br>
     */
    public static void main(String[] args) {
        RedisResponseDecoderV2 decoder = new RedisResponseDecoderV2();
        long t = System.currentTimeMillis();

        for (int i = 0; i < 1000000; i++) {
            // 整包数据
            // byte[] buffer1 = "+PONG \r\n".getBytes();
            // byte[] buffer2 = "-ERR Not implemented\r\n".getBytes();
            // byte[] buffer3 = ":2899\r\n".getBytes();
            // byte[] buffer4 = "$-1\r\n".getBytes();
            // byte[] buffer5 = "*2\r\n$7\r\npre1_bb\r\n$7\r\npre1_aa\r\n".getBytes();
            // byte[] buffer6 = "*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_aa\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_zz\r\n:2\r\n".getBytes();
            // System.out.println(decoder.decode(buffer1));
            // System.out.println(decoder.decode(buffer2));
            // System.out.println(decoder.decode(buffer3));
            // System.out.println(decoder.decode(buffer4));
            // System.out.println(decoder.decode(buffer5));
            // System.out.println(decoder.decode(buffer6));

            // 半包数据
            byte[] buffer = ("*3\r\n*4\r\n:5461\r\n:10922\r\n*3\r\n$15\r\n192.168.219.131\r\n:7003\r\n$40\r" +
                    "\n1fd8af2aa246c5adf00a25d1b6a0c1f4743bae5c\r\n" +
                    "*3\r\n$15\r\n192.168.219.132\r\n:7002\r\n$40\r\ne0b1c5791694fdc2ede655023e80f0e57b3d86b4\r\n*4\r\n:0\r\n:5460\r\n"
                    + "*3\r\n$15\r\n192.168.219.132\r\n:7000\r\n$40\r\nbee6866a13093c4411dea443ca8d901ea5d1e2f3\r\n" +
                    "*3\r\n$15\r\n192.168.219.131\r\n:7004\r\n$40\r\nb3ba9c1af0fa7296fe1e32f2a955879bcf79108b\r\n*4\r\n:10923\r\n:16383"
                    + "\r\n" + "*3\r\n$15\r\n192.168.219.132\r\n:7001\r\n$40\r\n9c86ec8088050f837c490aeda15aca5a2c85d7ef\r\n" +
                    "*3\r\n$15\r\n192.168.219.131\r\n:7005\r\n$40\r\nb0e22eccf79ced356e54a92ecbaa8d22757765d4\r\n").getBytes();

            byte[] buffer1 = new byte[buffer.length / 3];
            byte[] buffer2 = new byte[buffer.length / 3];
            byte[] buffer3 = new byte[buffer.length - buffer1.length - buffer2.length];

            System.arraycopy(buffer, 0, buffer1, 0, buffer1.length);
            System.arraycopy(buffer, buffer1.length, buffer2, 0, buffer2.length);
            System.arraycopy(buffer, buffer1.length + buffer2.length, buffer3, 0, buffer3.length);

            List<RedisResponse> resp;
            decoder.decode(buffer1);
            decoder.decode(buffer2);
            resp = decoder.decode(buffer3);
            // System.out.println(resp);
        }
        System.out.println("Decode costs " + (System.currentTimeMillis() - t) + " ms");
        System.out.println("Find costs " + decoder.findCost + " ms");
        System.out.println("read costs " + decoder.readCost + " ms");
    }
}