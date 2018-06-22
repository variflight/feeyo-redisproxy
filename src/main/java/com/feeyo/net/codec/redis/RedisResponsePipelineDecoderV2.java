package com.feeyo.net.codec.redis;


import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.util.CompositeByteArray;

import java.util.ArrayList;
import java.util.List;

import static com.feeyo.net.codec.redis.RedisResponsePipelineDecoder.PipelineResponse;

/**
 * 在 RedisResponsePipelineDecoder 实现上使用 CompositeByteArray 代替 byte[]
 *
 * @see com.feeyo.net.codec.redis.RedisResponsePipelineDecoder
 */
public class RedisResponsePipelineDecoderV2 implements Decoder<PipelineResponse> {
    private CompositeByteArray byteArray;
    // 用于标记读取的位置
    private int readOffset;
    private List<Integer> index = new ArrayList<>();

    /**
     * 解析返回 数量、内容
     */
    @Override
    public PipelineResponse decode(byte[] buffer) {
        int result = 0;
        append(buffer);
        try {

            for (; ; ) {
                // 至少4字节 :1\r\n
                if (byteArray.remaining(readOffset) < 4) {
                    return new PipelineResponse(PipelineResponse.ERR, 0, null);
                }

                int length = byteArray.getByteCount();
                byte type = byteArray.get(readOffset++);
                switch (type) {
                    case '*': // 数组(Array), 以 "*" 开头,表示消息体总共有多少行（不包括当前行）, "*" 是具体行数
                    case '+': // 正确, 表示正确的状态信息, "+" 后就是具体信息
                    case '-': // 错误, 表示错误的状态信息, "-" 后就是具体信息
                    case ':': // 整数, 以 ":" 开头, 返回
                    case '$': // 批量字符串, 以 "$"
                        // 解析，只要不抛出异常，就是完整的一条数据返回
                        parseResponse(type);
                        result++;
                        index.add(readOffset);
                }

                if (length < readOffset) {
                    throw new IndexOutOfBoundsException("Not enough data.");
                } else if (length == readOffset) {
                    readOffset = 0;
                    return new PipelineResponse(PipelineResponse.OK, result, getResponses());
                }
            }
        } catch (IndexOutOfBoundsException e1) {
            // 捕获这个错误（没有足够的数据），等待下一个数据包
            readOffset = 0;
            index.clear();
            return new PipelineResponse(PipelineResponse.ERR, 0, null);
        }
    }

    private void parseResponse(byte type) {
        int end, offset;
        int length = byteArray.getByteCount();

        if (type == '+' || type == '-' || type == ':') {
            offset = readOffset;
            end = readCRLFOffset();     // 分隔符 \r\n
            readOffset = end;       // 调整偏移值

            if (end > length) {
                readOffset = offset;
                throw new IndexOutOfBoundsException("Wait for more data.");
            }
        } else if (type == '$') {
            offset = readOffset;

            // 大小为 -1的数据包被认为是 NULL
            int packetSize = readInt();
            if (packetSize == -1) {
                return; // 此处不减
            }

            end = readOffset + packetSize + 2;      // offset + data + \r\n
            readOffset = end;       // 调整偏移值

            if (end > length) {
                readOffset = offset - 1;
                throw new IndexOutOfBoundsException("Wait for more data.");
            }
        } else if (type == '*') {
            offset = readOffset;

            // 大小为 -1的数据包被认为是 NULL
            int packetSize = readInt();
            if (packetSize == -1) {
                return; // 此处不减
            }

            if (packetSize > byteArray.remaining(readOffset)) {
                readOffset = offset - 1;
                throw new IndexOutOfBoundsException("Wait for more data.");
            }

            // 此处多增加一长度，用于存储 *packetSize\r\n
            byte nType;
            for (int i = 1; i <= packetSize; i++) {
                if (offset + 1 >= length) {
                    throw new IndexOutOfBoundsException("Wait for more data.");
                }

                nType = byteArray.get(offset++);
                parseResponse(nType);
            }
        }
    }

    private int readInt() throws IndexOutOfBoundsException {
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

        return (int) size;
    }

    private int readCRLFOffset() throws IndexOutOfBoundsException {
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
        return offset;
    }

    private void append(byte[] newBuffer) {
        if (newBuffer == null) {
            return;
        }

        if (byteArray == null) {
            byteArray = new CompositeByteArray();
            return;
        }

        // large packet
        byteArray.add(newBuffer);
        index.clear();
        readOffset = 0;
    }

    // 获取分段后的response
    private byte[][] getResponses() {
        byte[][] result = new byte[index.size()][];

        for (int i = 0; i < index.size(); i++) {
            int start;
            if (i == 0) {
                start = 0;
            } else {
                start = index.get(i - 1);
            }
            int end = index.get(i);

            result[i] = byteArray.subArray(start, end - start);
        }

        index.clear();
        byteArray.clear();
        return result;
    }

    /**
     * 性能测试结果： <br>
     * TODO 性能反而不如V1版本???
     * 响应包长度为538 <br>
     * 循环1kw次解析半包耗时约47s, V1版本约为34s(与包的数量有关) <br>
     * 循环1kw次解析整包耗时约27s, V1版本约为10s <br>
     */
    public static void main(String[] args) {
        RedisResponsePipelineDecoderV2 decoder = new RedisResponsePipelineDecoderV2();
        long t = System.currentTimeMillis();

        for (int i = 0; i < 10000000; i++) {
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

            // byte[] buffer1 = new byte[buffer.length / 3];
            // byte[] buffer2 = new byte[buffer.length / 3];
            // byte[] buffer3 = new byte[buffer.length - buffer1.length - buffer2.length];
            // System.arraycopy(buffer, 0, buffer1, 0, buffer1.length);
            // System.arraycopy(buffer, buffer1.length, buffer2, 0, buffer2.length);
            // System.arraycopy(buffer, buffer1.length + buffer2.length, buffer3, 0, buffer3.length);

            long t1 = System.currentTimeMillis();
            PipelineResponse resp;
            // decoder.decode(buffer1);
            // decoder.decode(buffer2);
            // resp = decoder.decode(buffer3);
            resp = decoder.decode(buffer);
            long t2 = System.currentTimeMillis();
            int diff = (int) (t2 - t1);
            if (diff > 1) {
                System.out.println(" decode diff=" + diff + ", response=" + resp);
            }
        }
        System.out.println("Decode costs " + (System.currentTimeMillis() - t) + " ms");
    }
}