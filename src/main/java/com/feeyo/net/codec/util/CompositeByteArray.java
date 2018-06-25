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
    // private ComponentAllocator allocator;
    private List<Component> components;
    // 用于标记读取的位置----还是放在外部维护比较简单,不然涉及到不停的read/write增加代码复杂度
    // private int readOffset;
    // 所有Component中byte[]长度之和, 用于检查下标值越界
    private int byteCount;

    public CompositeByteArray() {
        // this.allocator = new ComponentAllocator(3);
        this.components = new ArrayList<>();
        this.byteCount = 0;
    }

    public void add(byte[] bytes) {
        byteCount += bytes.length;
        int beginIndex = 0;
        int size = components.size();
        if (size != 0) {
            Component prevComponent = components.get(size - 1);
            beginIndex = prevComponent.length + prevComponent.beginIndex;
        }
        // Component comp = allocator.alloc();
        // comp.config(bytes, bytes.length, beginIndex);
        Component comp = new Component(bytes, bytes.length, beginIndex);
        components.add(comp);
    }

    public byte get(int index) {
        Component c = findComponent(index);
        return c.bytes[index - c.beginIndex];
    }

    // 需要同时返回size和readOffset
    public long[] readInt(int beginOffset) {
        long[] result = new long[2];

        Component c = findComponent(beginOffset);
        int indexC = components.indexOf(c);
        int length = components.size();
        byte tempByte;
        long size = 0;
        boolean isNeg = false;
        int offset = beginOffset;

        outer: for (; indexC < length; indexC++) {
            c = components.get(indexC);
            byte[] tempArr = c.bytes;
            int tempLength = tempArr.length;

            for (int j = 0; j < tempLength; j ++) {
                // 这里需要忽略掉offset之前的字节
                if (j + c.beginIndex < beginOffset) {
                    continue;
                }

                tempByte = tempArr[j];
                // 如果遇到\r表示读完了
                if ('\r' == tempByte) {
                    break outer;
                }

                if ('-' == tempByte) {
                    isNeg = true;
                } else {
                    size = size * 10 + tempByte - '0';
                }
                offset++;
            }
        }

        result[0] = isNeg ? -size : size;
        result[1] = offset;

        return result;
    }

    // 同时查找2个字节
    public int indexAdjacentTwoByte(int beginOffset, byte first, byte next) {
        Component c = findComponent(beginOffset);
        int indexC = components.indexOf(c);
        int length = components.size();
        byte[] tempArr;
        int tempLength;
        int firstIndex = -1;

        for (; indexC < length; indexC++) {

            c = components.get(indexC);
            tempArr = c.bytes;
            tempLength = tempArr.length;
            for (int j = 0; j < tempLength; j ++) {
                // 这里需要忽略掉offset之前的字节
                if (j + c.beginIndex < beginOffset) {
                    continue;
                }

                // 如果下一个字节不是next的话清除first的值
                if (firstIndex != -1) {
                    if (next == tempArr[j]) {
                        return j + c.beginIndex;
                    }
                    else {
                        firstIndex = -1;
                    }
                }

                if (first == tempArr[j]) {
                    firstIndex = j + c.beginIndex;
                }
            }
        }
        return -1;
    }

    // 从offset位置开始查找
    public int firstIndex(int paramOffset, byte value) {
        checkIndex(paramOffset, 1);

        Component c;
        int indexC;
        // 解析的请求大部分都是整包, 省略一次查找的过程
        if (paramOffset == 0) {
            indexC = 0;
        } else {
            c = findComponent(paramOffset);
            indexC = components.indexOf(c);
        }

        int length = components.size();
        byte[] tempArr;
        int tempLength;
        for (; indexC < length; indexC++) {

            c = components.get(indexC);
            tempArr = c.bytes;
            tempLength = tempArr.length;
            for (int j = 0; j < tempLength; j ++) {
                // 这里需要忽略掉offset之前的字节
                if (j + c.beginIndex < paramOffset) {
                    continue;
                }
                if (value == tempArr[j]) {
                    return j + c.beginIndex;
                }
            }
        }
        return -1;
    }

    // 取出指定区间的byte[], 可能跨多个Component
    public byte[] subArray(int beginIndex, int length) {
        checkIndex(beginIndex, length);

        byte[] resultArr = new byte[length];
        Component c = findComponent(beginIndex);
        int srcBeginIndex = beginIndex - c.beginIndex;
        // 判断c中剩余字节是否足够
        int availableByteCount = c.length - srcBeginIndex;

        // 一般都是在一个byte[]中的，这里先取一次如果不够再while循环处理
        if (availableByteCount >= length) {
            System.arraycopy(c.bytes, srcBeginIndex, resultArr, 0, length);
        } else {
            // 把c有效字节全部拷贝
            System.arraycopy(c.bytes, srcBeginIndex, resultArr, 0, availableByteCount);

            int destPos = availableByteCount;
            int remaining = length - availableByteCount;
            // 利用列表和byte[]顺序存放特性取剩下的字节
            int index = components.indexOf(c);

            while (remaining > 0) {
                c = components.get(++index);

                if (remaining <= c.length) {
                    System.arraycopy(c.bytes, 0, resultArr, destPos, remaining);
                    break;
                } else {
                    System.arraycopy(c.bytes, 0, resultArr, destPos, c.length);
                }

                destPos += c.length;
                remaining -= c.length;
            }
        }

        return resultArr;
    }

    /**
     * 返回剩余可读字节数
     */
    public int remaining(int readOffset) {
        return (byteCount - readOffset) < 0 ? 0 : (byteCount - readOffset);
    }

    public int getByteCount() {
        return byteCount;
    }

    /**
     * 清空其管理的所有byte[]并重置index <br>
     */
    public void clear() {
        // for (Component c : components) {
        //     c.invalid();
        // }
        components.clear();
        byteCount = 0;
    }

    private Component findComponent(int offset) {
        checkIndex(offset);

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

        throw new Error("should not reach here");
    }

    private void checkIndex(int index, int length) {
        if (isOutOfBounds(index, length, byteCount)) {
            throw new IndexOutOfBoundsException(String.format("index: %d, length: %d (expected: range(0, %d))", index, length,
                    byteCount));
        }
    }

    private boolean isOutOfBounds(int index, int length, int capacity) {
        return (index | length | (index + length) | (capacity - (index + length))) < 0;
    }

    private void checkIndex(int index) {
        checkIndex(index, 1);
    }

    // 包装了 byte[], 增加了 length 和 beginIndex 方便查找
    private final class Component {
        final byte[] bytes;
        final int length;
        final int beginIndex;
        // boolean isUsed;

        public Component(byte[] bytes, int length, int endIndex) {
            this.bytes = bytes;
            this.length = length;
            this.beginIndex = endIndex;
            // this.isUsed = true;
        }
        // public Component() {
        //     this.isUsed = false;
        // }
        //
        // public void config(byte[] bytes, int length, int endIndex) {
        //     this.bytes = bytes;
        //     this.length = length;
        //     this.beginIndex = endIndex;
        //     this.isUsed = true;
        // }
        //
        // public void invalid() {
        //     // 释放字节数组
        //     this.bytes = null;
        //     this.length = -1;
        //     this.beginIndex = -1;
        //     this.isUsed = false;
        // }
    }

    // 池化分配Component
    // private final class ComponentAllocator {
    //     private Component[] components;
    //     private int useIndex;
    //
    //     public ComponentAllocator(int poolSize) {
    //         useIndex = 0;
    //         components = new Component[poolSize];
    //         for (int i = 0; i < poolSize; i++) {
    //             components[i] = new Component();
    //         }
    //     }
    //
    //     public Component alloc() {
    //         int length = components.length;
    //         if (useIndex == length - 1) {
    //             useIndex = 0;
    //         }
    //
    //         Component component = components[useIndex];
    //         if (!component.isUsed) {
    //             useIndex++;
    //             return component;
    //         } else {
    //             int searchCount = 0;
    //
    //             // 上面的if已经算搜过一次了
    //             while (searchCount < length - 1) {
    //                 // 构造环形数组
    //                 if (useIndex == length - 1) {
    //                     useIndex = 0;
    //                 }
    //                 searchCount++;
    //                 useIndex++;
    //                 component = components[useIndex];
    //
    //                 // 找到可用的就返回
    //                 if (!component.isUsed) {
    //                     return component;
    //                 }
    //             }
    //
    //             // 已经没有可用的重新创建
    //             Component[] newArray = new Component[length * 2];
    //             System.arraycopy(components, 0, newArray, 0, length);
    //             components = newArray;
    //             for (int i = length; i < length * 2; i++) {
    //                 components[i] = new Component();
    //             }
    //
    //             return components[length];
    //         }
    //     }
    // }
}