package com.feeyo.net.codec.redis;

import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.UnknowProtocolException;
import com.feeyo.net.codec.util.CompositeByteArray;
import com.feeyo.net.codec.util.CompositeByteArray.ByteArray;

import java.util.ArrayList;
import java.util.List;

//
//
public class RedisRequestDecoderV2 implements Decoder<List<RedisRequest>> {
    
	private RedisRequest request = null;
    
	private CompositeByteArray compositeArray = null;
    
    // 用于标记读取的位置
    private int readOffset;
    
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
						int index = compositeArray.firstIndex(readOffset, (byte) '*');
						if (index == -1) {
							throw new IndexOutOfBoundsException("Not enough data.");
						} else {
							readOffset = index;
						}
                         
                        request = new RedisRequest();
                        state = State.READ_INIT;
                        break;
                    }
                    case READ_INIT: {
                        if (readOffset >= compositeArray.getByteCount() || (argCount != 0 && argCount == argIndex + 1)) {
                            state = State.READ_END;
                            break;
                        }
                        // 开始读，根据*/$判断是参数的数量还是参数命令/内容的长度
                        byte commandBeginByte = compositeArray.get(readOffset);
                        if (commandBeginByte == '*') {
                            readOffset++;
                            state = State.READ_ARG_COUNT;

                        } else if (commandBeginByte == '$') {
                            readOffset++;
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
                        request.getArgs()[argIndex] = compositeArray.getData(readOffset, argLength);
                        // argLength + 2(\r\n)
                        readOffset = readOffset + 2 + argLength;

                        this.state = State.READ_INIT;
                        break;
                    }
                    case READ_END: {
                        // 处理粘包
                        if (compositeArray.getByteCount() < readOffset) {
                            throw new IndexOutOfBoundsException("Not enough data.");
                            
                        } else if (compositeArray.getByteCount() == readOffset) {
                            if (argCount == argIndex + 1) {
                                pipeline.add(request);
                                reset();
                                // 整包解析完成
                                break decode;
                                // 断包（目前异步读取到的都是整包数据）
                            } else {
                                state = State.READ_SKIP;
                                readOffset = 0;
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
            readOffset = 0;
            return null;
        }

        return pipeline;
    }


    private int readInt() throws IndexOutOfBoundsException {
    	
        long size = 0;
        boolean isNeg = false;

        ByteArray c = compositeArray.findByteArray( readOffset );
        byte b = c.get(readOffset);
        while (b != '\r') {
            
        	if (b == '-') {
                isNeg = true;
            } else {
                // 对于长度大于10以上的其实是多个字节存在, 需要考虑到位数所以需要 *10 的逻辑
                // (byte) '1' = 49 为了得到原始的数字需要减去 '0'
                size = size * 10 + b - '0';
            }
            readOffset++;
            
            // bound 检查
            boolean isInBoundary = c.isInBoundary(readOffset);
            if ( !isInBoundary ) {
            	c = c.getNext();
            	if ( c == null ) {
            		throw new IndexOutOfBoundsException("Not enough data.");
            	}
            }
            
            b = c.get(readOffset);
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

    /**
     * 增加字节流(一般用于读半包)
     */
    private void append(byte[] newBuffer) {
        if (newBuffer == null) {
            return;
        }

        if (compositeArray == null) {
            compositeArray = new CompositeByteArray();
        }

        compositeArray.add(newBuffer);
        readOffset = 0;
    }

    public void reset() {
        state = State.READ_SKIP;
        compositeArray.clear();
        readOffset = 0;
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
     * 性能测试结果： <br>
     * 包长度为61(普通的请求: 一次鉴权+一次hashtable长度查询) <br>
     * 循环1kw次解析半包耗时31s, V1版本约为72s <br>
     * 循环1kw次解析整包耗时5s, V1版本约为3s <br>
     *
     * 包长度为565(批量请求) <br>
     * 循环1kw次解析半包耗时137s, V1版本约为207s <br>
     * 循环1kw次解析整包耗时约36s, V1版本约为27s <br>
     */
    public static void main(String[] args) {
        RedisRequestDecoderV2 decoder = new RedisRequestDecoderV2();
        long t = System.currentTimeMillis();

        for (int j = 0; j < 10000000; j++) {
            try {
                // 没有半包数据
                byte[] buff = "*2\r\n$4\r\nAUTH\r\n$5\r\npwd01\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHER\r\n".getBytes();

                // byte[] buff = ("*2\r\n$4\r\nAUTH\r\n$5\r\npwd01\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHER\r\n" +
                //         "*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE0\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE1\r\n" +
                //         "*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE2\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE3\r\n" +
                //         "*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE4\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE5\r\n" +
                //         "*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE6\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE7\r\n" +
                //         "*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE8\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATHE9\r\n" +
                //         "*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATH10\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATH11\r\n" +
                //         "*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATH12\r\n*2\r\n$4\r\nhlen\r\n$15\r\nSPECIAL_WEATH13\r\n").getBytes();
                // byte[] buff1 = new byte[ 213 ];
                // byte[] buff2 = new byte[ 155 ];
                // byte[] buff3 = new byte[ buff.length - buff1.length - buff2.length ];
                // System.arraycopy(buff, 0, buff1, 0, buff1.length);
                // System.arraycopy(buff, buff1.length, buff2, 0, buff2.length);
                // System.arraycopy(buff, buff1.length + buff2.length, buff3, 0, buff3.length);

                long t1 = System.currentTimeMillis();
                // decoder.decode(buff1);
                // decoder.decode( buff2 );
                // List<RedisRequest> reqList = decoder.decode( buff3 );
                List<RedisRequest> reqList = decoder.decode( buff );
                long t2 = System.currentTimeMillis();
                int diff = (int) (t2 - t1);
                if (diff > 2) {
                    System.out.println(" decode diff=" + diff + ", request=" + reqList.toString());
                }
            } catch (UnknowProtocolException e) {
                e.printStackTrace();
            }
        }
        System.out.println(System.currentTimeMillis() - t);
    }
}