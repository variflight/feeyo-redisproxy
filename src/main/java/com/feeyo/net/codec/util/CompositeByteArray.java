package com.feeyo.net.codec.util;

import java.util.ArrayList;
import java.util.List;

/**
 * 封装n个物理意义的字节数组,提供逻辑意义上的字节数组
 *
 * @see "https://github.com/netty/netty/blob/4.1/buffer/src/main/java/io/netty/buffer/CompositeByteBuf.java"
 * @see "https://skyao.gitbooks.io/learning-netty/content/buffer/class_CompositeByteBuf.html"
 */
public class CompositeByteArray {
    private List<Component> components;
    // 所有Component中byte[]长度之和, 用于检查下标值越界
    private int byteCount;

    public CompositeByteArray() {
        this.components = new ArrayList<>();
        this.byteCount = 0;
    }

    public void add(byte[] bytes) {
        byteCount += bytes.length;
        int beginIndex = 0;
        int size = components.size();
        Component prevComponent = null;
        if (size != 0) {
            prevComponent = components.get(size - 1);
            beginIndex = prevComponent.length + prevComponent.beginIndex;
        }

        Component comp = new Component(bytes, bytes.length, beginIndex);
        components.add(comp);
        if (prevComponent != null) {
            prevComponent.setNext(comp);
        }
    }

    public byte get(int index) {
        Component c = findComponent(index);
        return c.get(index);
    }

    // 从offset位置开始查找
    public int firstIndex(int paramOffset, byte value) {
        checkIndex(paramOffset, 1);

        Component c = findComponent(paramOffset);
        return c.find(paramOffset, value);
    }

    /**
     * 取出指定区间的byte[], 可能跨多个Component
     *
     * @param beginComp  beginIndex所在的Component
     * @param beginIndex 截取的开始位置
     * @param length     需要截取的长度
     * @return byte[]
     */
    public byte[] subArray(Component beginComp, int beginIndex, int length) {
        // checkIndex(beginIndex, length);
        assert beginComp != null;

        byte[] resultArr = new byte[length];
        Component c = beginComp;
        int remaining = length;
        int destPos = 0;
        int subBeginIndex = beginIndex - c.beginIndex;
        int subLength;

        while (c != null && remaining > 0) {

            // 表示已经不是第一个Component, 总是从0开始copy
            if (remaining < length) {
                subBeginIndex = 0;
                // copy长度为Min(remaining, c.length)
                subLength = Math.min(remaining, c.length);
            } else {
                // 第一个Component时, c.length - subBeginIndex 为Component中剩余有效字节数
                subLength = Math.min(remaining, c.length - subBeginIndex);
            }

            System.arraycopy(c.bytes, subBeginIndex, resultArr, destPos, subLength);
            remaining = remaining - subLength;
            destPos = destPos + subLength;
            c = c.next;
        }

        return resultArr;
    }

    public byte[] subArray(int beginIndex, int length) {
        Component c = findComponent(beginIndex);
        return subArray(c, beginIndex, length);
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
        components.clear();
        byteCount = 0;
    }

    public Component findComponent(int offset) {
        // 依赖外部调用检查
        // checkIndex(offset, 1);

        // 二分查找
        for (int low = 0, high = components.size(); low <= high; ) {
            int mid = low + high >>> 1;
            Component c = components.get(mid);
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
            throw new IndexOutOfBoundsException(String.format("index: %d, length: %d (expected: range(0, %d))", index, length,
                    byteCount));
        }
    }

    // 包装了 byte[], 增加了 length 和 beginIndex 方便查找
    public final class Component {
        final byte[] bytes;
        final int length;
        final int beginIndex;
        // 优化遍历, 维护单向链表
        private Component next;

        public Component(byte[] bytes, int length, int endIndex) {
            this.bytes = bytes;
            this.length = length;
            this.beginIndex = endIndex;
        }

        public Component getNext() {
            return next;
        }

        public void setNext(Component next) {
            this.next = next;
        }

        // 检查是否在本Component范围
        public boolean isInRange(int index) {
            return index < beginIndex + length && index >= beginIndex;
        }

        // 向下冗余一个Component
        public byte get(int index) {
            // next为空则保持抛出ArrayIndexOutOfBoundsException
            if (index - beginIndex < length || next == null) {
                return bytes[index - beginIndex];
            }
            return next.get(index);
        }

        // Component内部实现查找方法
        public int find(int index, byte value) {
            // check index 有效性
            Component c = this;

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