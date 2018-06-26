package com.feeyo.net.codec.util;

import java.util.ArrayList;
import java.util.List;

/**
 * 封装n个物理意义的字节数组, 对外提供一个逻辑意义上的字节数组
 *
 * @see "https://github.com/netty/netty/blob/4.1/buffer/src/main/java/io/netty/buffer/CompositeByteBuf.java"
 * @see "https://skyao.gitbooks.io/learning-netty/content/buffer/class_CompositeByteBuf.html"
 */
public class CompositeByteChunk {
	
    private List<ByteChunk> chunks;
    
    private int byteCount;

    public CompositeByteChunk() {
        this.chunks = new ArrayList<>();
        this.byteCount = 0;
    }

    public void add(byte[] bytes) {
    	
        byteCount += bytes.length;
        
        int beginIndex = 0;
        int size = chunks.size();
        
        ByteChunk prevChunk = null;
        if (size != 0) {
            prevChunk = chunks.get(size - 1);
            beginIndex = prevChunk.length + prevChunk.beginIndex;
        }

        ByteChunk chunk = new ByteChunk(bytes, bytes.length, beginIndex);
        chunks.add( chunk );
        
        if (prevChunk != null) {
            prevChunk.setNext(chunk);
        }
    }

    public byte get(int index) {
        ByteChunk c = findChunk(index);
        return c.get(index);
    }

    // 从offset位置开始查找
    public int firstIndex(int paramOffset, byte value) {
    	
        checkIndex(paramOffset, 1);

        ByteChunk c = findChunk(paramOffset);
        return c.find(paramOffset, value);
    }

    
    public byte[] getData(int beginIndex, int length) {
        ByteChunk c = findChunk(beginIndex);
        return getData(c, beginIndex, length);
    }
    

    /* 
     	取出指定区间的byte[], 可能跨多个Chunk
     	beginIndex 截取的开始位置, length 需要截取的长度
    */
    public byte[] getData(ByteChunk chunk, int beginIndex, int length) {
    	
        assert chunk != null;
        
        byte[] destData = new byte[length];
        
        ByteChunk c = chunk;
        int remaining = length;
        int destPos = 0;
        
        int srcPos = beginIndex - c.beginIndex;
        int srcLength;

        while (c != null && remaining > 0) {

            // 是否第一次
            if (remaining < length) {
            	
            	// 否
                srcPos = 0;
                srcLength = Math.min(remaining, c.length);         		
                
            } else {
            	
                srcLength = Math.min(remaining, c.length - srcPos);  
            }

            System.arraycopy(c.data, srcPos, destData, destPos, srcLength);
            remaining = remaining - srcLength;
            destPos = destPos + srcLength;
            c = c.next;
        }

        return destData;
    }


    /**
     * 返回剩余可读字节数
     */
    public int remaining(int readOffset) {
        return Math.max(byteCount - readOffset, 0);
    }

    public int getByteCount() {
        return byteCount;
    }

    /**
     * 清空其管理的所有byte[]并重置index <br>
     */
    public void clear() {
        chunks.clear();
        byteCount = 0;
    }

    public ByteChunk findChunk(int offset) {
        // 依赖外部调用检查
        // checkIndex(offset, 1);

        // 二分查找
        for (int low = 0, high = chunks.size(); low <= high; ) {
            int mid = low + high >>> 1;
            ByteChunk c = chunks.get(mid);
            if (offset >= c.beginIndex + c.length) {
                low = mid + 1;
            } else if (offset < c.beginIndex) {
                high = mid - 1;
            } else {
                assert c.length != 0;
                return c;
            }
        }

        throw new IndexOutOfBoundsException("Not enough data.");
    }

    private void checkIndex(int index, int length) {
        if ((index | length | (index + length) | (byteCount - (index + length))) < 0) {
            throw new IndexOutOfBoundsException(
            		String.format("index: %d, length: %d (expected: range(0, %d))", index, length, byteCount));
        }
    }

    
    
    /*
      	包装了 byte[], 增加了 length 和 beginIndex 方便查找
     */
    public final class ByteChunk {
    	
        final byte[] data;
        
        final int beginIndex;
        final int length;
      
        // 优化遍历, 维护单向链表
        private ByteChunk next;

        public ByteChunk(byte[] data, int length, int beginIndex) {
            this.data = data;
            this.length = length;
            this.beginIndex = beginIndex;
        }

        public ByteChunk getNext() {
            return next;
        }

        public void setNext(ByteChunk next) {
            this.next = next;
        }

        // 边界检查
        public boolean isInBoundary(int index) {
            return  index >= beginIndex && index < (beginIndex + length);
        }

        // next为空则保持抛出ArrayIndexOutOfBoundsException
        public byte get(int index) {

            if (index - beginIndex < length || next == null) {
                return data[index - beginIndex];
            }
            return next.get(index);
        }

        public int find(int index, byte value) {
            
        	// check index 有效性
            ByteChunk c = this;

            // 利用链表和数组快速查找
            while (c != null) {
                while (index < c.beginIndex + c.length) {
                    if (value == c.get(index)) {
                        return index;
                    }
                    index++;
                }
                c = c.next;
            }
            return -1;
        }
    }
}