package com.feeyo.net.codec.redis;

import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.UnknowProtocolException;
import com.feeyo.net.codec.util.CompositeByteArray;

import java.util.ArrayList;
import java.util.List;

/**
 * 在RedisRequestDecoder实现上使用CompositeByteArray代替byte[]
 *
 * @see: com.feeyo.net.codec.redis.RedisRequestDecoder <br>
 */
public class RedisRequestDecoderV2 implements Decoder<List<RedisRequest>> {
	
    private RedisRequest request = null;
    private CompositeByteArray byteArray = null;
    
    private State state = State.READ_SKIP;

    @Override
    public List<RedisRequest> decode(byte[] buffer) throws UnknowProtocolException {
        append(buffer);

        // pipeline
        List<RedisRequest> pipeline = new ArrayList<>();

        try {
            // 读取到的参数索引
            int argIndex = -1;
            // 参数的数量
            int argCount = 0;
            // 参数的长度
            int argLength = 0;

            decode:
            for (; ; ) {
                switch (state) {
                    case READ_SKIP: {
                        // 找到请求开始的位置，redis协议中以*开始；找不到报错。可以解析多个请求
                        skipBytes();
                        request = new RedisRequest();
                        state = State.READ_INIT;
                        break;
                    }
                    case READ_INIT: {
                        int readIndex = byteArray.getReadOffset();
                        if (readIndex >= byteArray.getByteCount() || (argCount != 0 && argCount == argIndex + 1)) {
                            state = State.READ_END;
                            break;
                        }
                        // 开始读，根据*/$判断是参数的数量还是参数命令/内容的长度
                        byte commandBeginByte = byteArray.get(readIndex);
                        if (commandBeginByte == '*') {
                            byteArray.setReadOffset(++readIndex);
                            state = State.READ_ARG_COUNT;

                        } else if (commandBeginByte == '$') {
                            byteArray.setReadOffset(++readIndex);
                            state = State.READ_ARG_LENGTH;
                        }
                        break;
                    }
                    case READ_ARG_COUNT: {
                        argCount = readInt();
                        byte[][] args = new byte[argCount][];
                        request.setArgs(args);
                        this.state = State.READ_INIT;
                        break;
                    }
                    case READ_ARG_LENGTH: {
                        // 读取参数长度给下个阶段READ_ARG使用
                        argLength = readInt();
                        argIndex++;
                        this.state = State.READ_ARG;
                        break;
                    }
                    case READ_ARG: {
                        // 根据READ_ARG_LENGTH中读到的参数长度获得参数内容
                        int readIndex = byteArray.getReadOffset();
                        request.getArgs()[argIndex] = byteArray.subArray(readIndex, argLength);
                        // argLength + 2(\r\n)
                        byteArray.setReadOffset(readIndex + 2 + argLength);

                        this.state = State.READ_INIT;
                        break;
                    }
                    case READ_END: {
                        // 处理粘包
                        int readIndex = byteArray.getReadOffset();
                        if (byteArray.getByteCount() < readIndex) {
                            throw new IndexOutOfBoundsException("Not enough data.");
                        } else if (byteArray.getByteCount() == readIndex) {
                            if (argCount == argIndex + 1) {
                                pipeline.add(request);
                                reset();
                                // 整包解析完成
                                break decode;
                                // 断包（目前异步读取到的都是整包数据）
                            } else {
                                state = State.READ_SKIP;
                                byteArray.setReadOffset(0);
                                return null;
                            }
                        } else {
                            argIndex = -1;
                            argCount = 0;
                            argLength = 0;
                            pipeline.add(request);
                            this.state = State.READ_SKIP;
                        }
                    }
                    break;
                    default:
                        throw new UnknowProtocolException("Unknown state: " + state);
                }
            }
        } catch (IndexOutOfBoundsException e) {
            state = State.READ_SKIP;
            byteArray.setReadOffset(0);
            return null;
        }

        return pipeline;
    }

    /**
     * 如果第一个字符不是*则skip直到遇到*
     */
    private void skipBytes() {
        int readIndex = byteArray.getReadOffset();
        int index = byteArray.firstIndex(readIndex, (byte) '*');
        if (index == -1) {
            throw new IndexOutOfBoundsException("Not enough data.");
        } else {
            byteArray.setReadOffset(index);
        }
    }

    private int readInt() throws IndexOutOfBoundsException {
        int readIndex = byteArray.getReadOffset();
        long size = 0;
        boolean isNeg = false;

        int tempIndex = readIndex;
        byte b = byteArray.get(tempIndex);
        while (b != '\r') {
            if (b == '-') {
                isNeg = true;
            } else {
                // TODO 什么意思
                size = size * 10 + b - '0';
            }
            tempIndex++;
            b = byteArray.get(tempIndex);
        }

        // skip \r\n
        byteArray.setReadOffset(tempIndex + 2);

        size = (isNeg ? -size : size);
        if (size > Integer.MAX_VALUE) {
            throw new RuntimeException("Cannot allocate more than " + Integer.MAX_VALUE + " bytes");
        }
        if (size < Integer.MIN_VALUE) {
            throw new RuntimeException("Cannot allocate less than " + Integer.MIN_VALUE + " bytes");
        }

        return (int) size;
    }

    /**
     * 增加字节流(一般用于读半包)
     */
    private void append(byte[] newBuffer) {
        if (newBuffer == null) {
            return;
        }

        if (byteArray == null) {
            byteArray = new CompositeByteArray();
        }

        byteArray.add(newBuffer);
    }

    public void reset() {
        state = State.READ_SKIP;
        byteArray.clear();
    }

    private enum State {
        READ_SKIP,            // 跳过空格
        READ_INIT,            // 开始
        READ_ARG_COUNT,    // 读取参数数量(新协议)
        READ_ARG_LENGTH,    // 读取参数长度(新协议)
        READ_ARG,            // 读取参数(新协议)
        READ_END            // 结束
    }

    /**
     * 性能测试结果：当没有半包数据时性能与之前没有太多变化甚至略差
     * 当全是半包数据时性能会好很多. 循环1kw次耗时31s, 约等于之前耗时的一半
     */
    public static void main(String[] args) {
        RedisRequestDecoderV2 decoder = new RedisRequestDecoderV2();
        long t = System.currentTimeMillis();

        for (int j = 0; j < 10000000; j++) {
            try {
                // 没有半包数据
                // String msg = "*2\r\n$4\r\nAUTH\r\n$5\r\npwd01\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHER\r\n*1\r\n$4\r\nKEYS\r\n";
                // 半包数据
                byte[] buff = "*2\r\n$3\r\nGET\r\n$2\r\naa\r\n".getBytes();
                byte[] buff1 = new byte[ 4 ];
                byte[] buff2 = new byte[ 4 ];
                byte[] buff3 = new byte[ buff.length - 8 ];
                System.arraycopy(buff, 0, buff1, 0, 4);
                System.arraycopy(buff, 4, buff2, 0, 4);
                System.arraycopy(buff, 8, buff3, 0, buff3.length);

                long t1 = System.currentTimeMillis();
                decoder.decode(buff1);
                decoder.decode( buff2 );
                List<RedisRequest> reqList = decoder.decode( buff3 );
                long t2 = System.currentTimeMillis();
                int diff = (int) (t2 - t1);
                if (diff > 1) {
                    System.out.println(" decode diff=" + diff + ", request=" + reqList.toString());
                }

            } catch (UnknowProtocolException e) {
                e.printStackTrace();
            }
        }
        System.out.println(System.currentTimeMillis() - t);
    }
}