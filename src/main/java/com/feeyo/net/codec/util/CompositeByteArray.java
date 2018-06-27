package com.feeyo.net.codec.util;

import java.util.ArrayList;
import java.util.List;

/**
 * 封装n个物理意义的字节数组, 对外提供一个逻辑意义上的字节数组
 *
 * @see "https://github.com/netty/netty/blob/4.1/buffer/src/main/java/io/netty/buffer/CompositeByteBuf.java"
 */
public class CompositeByteArray {
	
    private List<ByteArray> chunks = new ArrayList<>();
    private int byteCount = 0;
    
    private ByteArray lastChunk = null;

    //
    public void add(byte[] data) {
    	
        this.byteCount += data.length;
        
        ByteArray c = null;
        if ( lastChunk != null ) {
            c = new ByteArray(data, data.length, (lastChunk.length + lastChunk.beginIndex));
            lastChunk.setNext( c );
            
        } else {
        	c = new ByteArray(data, data.length, 0);
        }
        
        chunks.add( c );
        lastChunk = c;
    }

    public byte get(int index) {
        ByteArray c = findByteArray(index);
        return c.get(index);
    }

    // 从offset位置开始查找 指定 byte 
    public int firstIndex(int paramOffset, byte value) {
        checkIndex(paramOffset, 1);

        ByteArray c = findByteArray(paramOffset);
        return c.find(paramOffset, value);
    }

    /* 
	 	取出指定区间的byte[], 可能跨多个Chunk
	 	beginIndex 截取的开始位置, length 需要截取的长度
	*/
    public byte[] getData(int beginIndex, int length) {
    	// 
        ByteArray c = findByteArray(beginIndex);
        return getData(c, beginIndex, length);
    }
    
    public byte[] getData(ByteArray chunk, int beginIndex, int length) {
    	
        assert chunk != null;
        
        byte[] destData = new byte[length];
        
        ByteArray c = chunk;
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
        lastChunk = null;
        byteCount = 0;
    }

    public ByteArray findByteArray(int offset) {
        // 依赖外部调用检查
        // checkIndex(offset, 1);

        // 二分查找
        for (int low = 0, high = chunks.size(); low <= high; ) {
            int mid = low + high >>> 1;
            ByteArray c = chunks.get(mid);
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
    public final class ByteArray {
    	
        final byte[] data;
        
        final int beginIndex;
        final int length;
      
        // 优化遍历, 维护单向链表
        private ByteArray next;

        public ByteArray(byte[] data, int length, int beginIndex) {
            this.data = data;
            this.length = length;
            this.beginIndex = beginIndex;
        }

        public ByteArray getNext() {
            return next;
        }

        public void setNext(ByteArray next) {
            this.next = next;
        }

        // 边界检查
        public boolean isInBoundary(int index) {
            return  index >= beginIndex && index < (beginIndex + length);
        }

        public byte get(int index) {
        	// TODO: next为空则保持抛出ArrayIndexOutOfBoundsException
            if ( (index - beginIndex) < length || next == null) {
                return data[index - beginIndex];
            }
            
            return next.get(index);
        }

        public int find(int index, byte value) {
            
        	// check index 有效性
            ByteArray c = this;

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