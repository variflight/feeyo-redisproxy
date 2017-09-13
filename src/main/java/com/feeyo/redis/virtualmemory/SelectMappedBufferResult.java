package com.feeyo.redis.virtualmemory;

import java.nio.ByteBuffer;

/**
 * 查询 Page cache 返回结果
 *
 */
public class SelectMappedBufferResult {

	// 从队列中哪个绝对Offset开始
    private final long startOffset;
    
    // position从0开始
    private final ByteBuffer byteBuffer;
    
    // 有效数据大小
    private int size;
    
    // 用来释放内存
    private MappedFile mappedFile;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }

    public MappedFile getMappedFile() {
        return mappedFile;
    }

//    @Override
//    protected void finalize() {
//        if (this.mappedFile != null) {
//            this.release();
//        }
//    }
    
    /**
     * 此方法只能被调用一次，重复调用无效
     */
    public synchronized void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }

    public long getStartOffset() {
        return startOffset;
    }
}