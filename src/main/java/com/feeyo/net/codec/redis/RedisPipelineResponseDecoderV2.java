package com.feeyo.net.codec.redis;


import com.feeyo.net.codec.Decoder;
import com.feeyo.net.codec.util.CompositeByteChunkArray;
import com.feeyo.net.codec.util.CompositeByteChunkArray.ByteChunk;

import java.util.ArrayList;
import java.util.List;

//
//
public class RedisPipelineResponseDecoderV2 implements Decoder<RedisPipelineResponse> {
    
	private CompositeByteChunkArray chunkArray;
    
    // 用于标记读取的位置
    private int readOffset;
    
    private ByteChunk readChunk;
    private ByteChunk startChunk;
    
    private List<Integer> index = new ArrayList<>();

    /**
     * 解析返回 数量、内容
     */
    @Override
    public RedisPipelineResponse decode(byte[] buffer) {
        int result = 0;
        append(buffer);
        
        try {

            // 批量回复时会更新startChunk,导致最后从0开始的取数据异常
            startChunk = readChunk = chunkArray.findChunk(readOffset);
            for (; ; ) {
                // 至少4字节 :1\r\n
                if (chunkArray.remaining(readOffset) < 4) {
                    return new RedisPipelineResponse(RedisPipelineResponse.ERR, 0, null);
                }

                // 减少 findChunk 的调用次数
                byte type = readChunk.get(readOffset++);
                updateReadOffsetAndReadByteChunk(readOffset);
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

                if ( chunkArray.getByteCount() < readOffset) {
                    throw new IndexOutOfBoundsException("Not enough data.");
                    
                } else if ( chunkArray.getByteCount() == readOffset) {
                    readOffset = 0;
                    return new RedisPipelineResponse(RedisPipelineResponse.OK, result, getResponses());
                }
            }
        } catch (IndexOutOfBoundsException e1) {
            // 捕获这个错误（没有足够的数据），等待下一个数据包
            readOffset = 0;
            index.clear();
            return new RedisPipelineResponse(RedisPipelineResponse.ERR, 0, null);
        }
    }

    private void parseResponse(byte type) {
        int end, offset;

        if (type == '+' || type == '-' || type == ':') {
            offset = readOffset;
            end = readCRLFOffset();     // 分隔符 \r\n
            updateReadOffsetAndReadByteChunk(end);     // 调整偏移值

            if (end > chunkArray.getByteCount()) {
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
            updateReadOffsetAndReadByteChunk(end);   // 调整偏移值

            if (end > chunkArray.getByteCount() ) {
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

            if (packetSize > chunkArray.remaining(readOffset)) {
                readOffset = offset - 1;
                throw new IndexOutOfBoundsException("Wait for more data.");
            }

            // 此处多增加一长度，用于存储 *packetSize\r\n
            byte nType;
            for (int i = 1; i <= packetSize; i++) {
                if (offset + 1 >= chunkArray.getByteCount() ) {
                    throw new IndexOutOfBoundsException("Wait for more data.");
                }

                nType = readChunk.get(readOffset++);
                updateReadOffsetAndReadByteChunk(readOffset);
                // nType = byteArray.get(offset++);
                parseResponse(nType);
            }
        }
    }

    private int readInt() throws IndexOutOfBoundsException {
        long size = 0;
        boolean isNeg = false;

        if (readOffset >= chunkArray.getByteCount() ) {
            throw new IndexOutOfBoundsException("Not enough data.");
        }

        ByteChunk c = readChunk;
        byte b = c.get(readOffset);
        outer: while (c != null) {

            while (c.isInBoundary(readOffset)) {

                if (b == '\r') {
                    break outer;
                }

                if (b == '-') {
                    isNeg = true;
                } else {
                    size = size * 10 + b - '0';
                }
                readOffset++;

                if (readOffset >= chunkArray.getByteCount() ) {
                    throw new IndexOutOfBoundsException("Not enough data.");
                }
                b = c.get(readOffset);
            }
            c = c.getNext();
        }

        // 加上\r\n 之后更新 readByteChunk
        updateReadOffsetAndReadByteChunk(readOffset + 2);

        size = (isNeg ? -size : size);
        if (size > Integer.MAX_VALUE) {
            throw new RuntimeException("Cannot allocate more than " + Integer.MAX_VALUE + " bytes");
        }
        if (size < Integer.MIN_VALUE) {
            throw new RuntimeException("Cannot allocate less than " + Integer.MIN_VALUE + " bytes");
        }

        return (int) size;
    }

    // 在遍历中改变readOffset可能需要更新 readByteChunk
    private void updateReadOffsetAndReadByteChunk(int newReadOffset) {
    	
        readOffset = newReadOffset;
        
        while (readChunk != null) {
            // 当offset达到最大长度时也不继续,防止空指针异常
            if (readChunk.isInBoundary(readOffset) || readOffset == chunkArray.getByteCount() ) {
                return;
            }
            readChunk = readChunk.getNext();
        }
    }

    private void updateStartByteChunk(int reachOffset) {
        while (startChunk != null) {
            // 当offset达到最大长度时也不继续,防止空指针异常
            if (startChunk.isInBoundary(reachOffset) || reachOffset == chunkArray.getByteCount() ) {
                return;
            }
            startChunk = startChunk.getNext();
        }
    }

    private void append(byte[] newBuffer) {
        if (newBuffer == null) {
            return;
        }

        if (chunkArray == null) {
            chunkArray = new CompositeByteChunkArray();
        }

        // large packet
        chunkArray.add(newBuffer);
        index.clear();
        readOffset = 0;
    }

    /**
     * 往前查找第一个 '\r\n' 对应的offset值
     *
     * @return '\r\n' 对应的offset值
     * @throws IndexOutOfBoundsException
     */
    private int readCRLFOffset() throws IndexOutOfBoundsException {
        int offset = readOffset;
        if (offset + 1 >= chunkArray.getByteCount()) {
            throw new IndexOutOfBoundsException("Not enough data.");
        }

        ByteChunk c = readChunk;
        outer: while (c != null) {

            while (c.isInBoundary(offset)) {

                if (c.get(offset) == '\r' && c.get(offset + 1) == '\n') {
                    break outer;
                }
                offset++;

                if (offset + 1 == chunkArray.getByteCount()) {
                    throw new IndexOutOfBoundsException("didn't see LF after NL reading multi bulk count (" + offset + " => " +
                    		chunkArray.getByteCount() + ", " + readOffset + ")");
                }
            }
            c = c.getNext();
        }

        offset++;
        offset++;

        return offset;
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

            updateStartByteChunk(start);
            result[i] = chunkArray.getData(startChunk, start, end - start);
        }

        index.clear();
        chunkArray.clear();
        return result;
    }

    /**
     * 性能测试结果： <br>
     *
     * 响应包长度为538 <br>
     * 循环1kw次解析半包耗时约29s, V1版本约为32s(与包的数量有关) <br>
     * 循环1kw次解析整包耗时约12s, V1版本约为10s <br>
     */
    public static void main(String[] args) {
        RedisPipelineResponseDecoderV2 decoder = new RedisPipelineResponseDecoderV2();
        long t = System.currentTimeMillis();
       // PipelineResponse resp;

        for (int i = 0; i < 10000000; i++) {
            // 整包数据
            // byte[] buffer1 = "+PONG \r\n".getBytes();
            // byte[] buffer2 = "-ERR Not implemented\r\n".getBytes();
            // byte[] buffer3 = ":2899\r\n".getBytes();
            // byte[] buffer4 = "$-1\r\n".getBytes();
            // byte[] buffer5 = "*2\r\n$7\r\npre1_bb\r\n$7\r\npre1_aa\r\n".getBytes();
            // byte[] buffer6 = "*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_aa\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$7\r\npre1_zz\r\n:2\r\n".getBytes();

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
            // decoder.decode(buffer1);
            // decoder.decode(buffer2);
            // resp = decoder.decode(buffer3);
            
            decoder.decode(buffer);
            
            //resp = decoder.decode(buffer);
            // System.out.println("decode pipeline response=" + resp.isOK());
            // for (byte[] r : resp.getResps()) {
            //     System.out.println(new String(r));
            // }
        }
        System.out.println("Decode costs " + (System.currentTimeMillis() - t) + " ms");
    }
}