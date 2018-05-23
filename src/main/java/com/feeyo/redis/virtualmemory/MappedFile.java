package com.feeyo.redis.virtualmemory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.virtualmemory.AppendMessageResult.AppendMessageStatus;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;


/*
  Page Cache 文件访问封装
 */
public class MappedFile extends ReferenceResource {
	
    protected static final Logger log = LoggerFactory.getLogger( MappedFile.class );

    //默认页大小为4k
    public static final int OS_PAGE_SIZE = 1024 * 4;
    
    // 当前JVM中映射的虚拟内存总大小
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    // 当前JVM中mmap句柄数量
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    
    //当前写文件的位置
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    
    protected int fileSize;
    protected FileChannel fileChannel;

    // 映射的文件名
    private String fileName;
    
    // 映射的起始偏移量
    private long fileFromOffset;
    
    // 映射的文件
    private File file;
    
    // 映射的内存对象，position永远不变
    private MappedByteBuffer mappedByteBuffer;
    
    // 最后一条消息存储时间
    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;

    //+++++++++++++逻辑上的消费标记+++++++++++++++
    private final AtomicBoolean isAbleToMarked = new AtomicBoolean(true);
    private BitSet consumedTrack;
   
    public MappedFile() {
    }

    @SuppressWarnings("resource")
	public MappedFile(final String fileName, final int fileSize) throws IOException {

    	 this.fileName = fileName;
         this.fileSize = fileSize;
         this.file = new File(fileName);
         this.fileFromOffset = Long.parseLong(this.file.getName());
         boolean ok = false;

         ensureDirOK(this.file.getParent());

         try {
             this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
             this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
             TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
             TOTAL_MAPPED_FILES.incrementAndGet();
             consumedTrack = new BitSet(fileSize);
             consumedTrack.clear(0,fileSize);
             ok = true;
         } catch (FileNotFoundException e) {
             log.error("create file channel " + this.fileName + " Failed. ", e);
             throw e;
         } catch (IOException e) {
             log.error("map file " + this.fileName + " Failed. ", e);
             throw e;
         } finally {
             if (!ok && this.fileChannel != null) {
                 this.fileChannel.close();
             }
         }
    }
    
    //
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        // JDK7中将DirectByteBuffer类中的viewedBuffer方法换成了attachment方法
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    // 获取文件大小
    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 向MapedBuffer追加消息<br>
     * 
     * @param msg 要追加的消息
     * @param cb 用来对消息进行序列化，尤其对于依赖MapedFile Offset的属性进行动态序列化
     * @return 是否成功，写入多少数据
     */
    public AppendMessageResult appendMessage(final Message msg, final AppendMessageCallback cb) {
        assert msg != null;
        assert cb != null;
        
        int currentPos = this.wrotePosition.get();
        
        // 表示有空余空间
        if (currentPos < this.fileSize) {            
        	ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            
            AppendMessageResult result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, msg);
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        
        // 上层应用应该保证不会走到这里
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos,  this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * 文件起始偏移量
     */
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 向存储层追加数据
     * @return 返回写入了多少数据
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();
        // 表示有空余空间
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }
        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param data
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     * @return
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 考虑到写入性能，满足 flushLeastPages * OS_PAGE_SIZE 才进行 flush
     * 
     * @param flushLeastPages  flush最小页数
     * 
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if ( this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 是否能够flush。满足如下条件任意条件：
     * 1. 映射文件已经写满
     * 2. flushLeastPages > 0 && 未flush部分超过flushLeastPages
     * 3. flushLeastPages = 0 && 有新写入部分
     * 
     * @param flushLeastPages flush最小分页
     * @return 是否能够写入
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();
        
        // 如果当前文件已经写满，应该立刻刷盘
        if (this.isFull()) {
            return true;
        }

        // 只有未刷盘数据满足指定page数目才刷盘
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }
    
    public boolean isConsumedAll() {
        if (!isFull())
            return false;
        int lastConsumedWhere = consumedTrack.nextClearBit(0);
        return lastConsumedWhere >= fileSize ? true : false;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        
        // 有消息
        if ((pos + size) <= readPosition) {
        	 // 从MapedBuffer读
            if (this.hold()) {
            	
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                SelectMappedBufferResult sbr = new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
                return sbr;
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
       
        // 请求参数非法
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }
        return null;
    }

    /**
     * 读逻辑分区
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                SelectMappedBufferResult sbr = new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
                return sbr;
            }
        }
        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
    	// 如果没有被shutdown，则不可以unmap文件，否则会crash
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have not shutdown, stop unmapping.");
            return false;
        }

        // 如果已经cleanup，再次操作会引起crash
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have cleanup, do not do it again.");
            // 必须返回true
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 清理资源，destroy与调用shutdown的线程必须是同一个
     * 
     * @return 是否被destory成功，上层调用需要对失败情况处理，失败后尝试重试
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + Util.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.wrotePosition.get();
    }

    public void warmMappedFile(int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);
        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    // 确认消费标记
    public void markAsConsumed(int offset, int size) {
        while (!this.isAbleToMarked.compareAndSet(true, false)) {
            Thread.yield();
        }
        
        try {
            consumedTrack.set(offset,  offset + size);
        } finally {
            isAbleToMarked.set(true);
        }
    }

    /**
     * 方法不能在运行时调用，不安全。只在启动时，reload已有数据时调用
     */
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /*
     	mlock方法在标准C中的实现是将锁住指定的内存区域避免被操作系统调到swap空间中
     	madvise的作用是一次性先将一段数据读入到映射内存区域，这样就减少了缺页异常的产生
     */
    @SuppressWarnings("restriction")
	public void mlock() {
        final long beginTime = System.currentTimeMillis();
		final long address = ((sun.nio.ch.DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = PlatformLibC.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", new Object[]{ address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime } );
        }

        {
            int ret = PlatformLibC.madvise(pointer, new NativeLong(this.fileSize), PlatformLibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}",  new Object[]{ address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime });
        }
    }

    @SuppressWarnings("restriction")
	public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((sun.nio.ch.DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = PlatformLibC.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}",  new Object[]{ address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime });
    }

    @Override
    public String toString() {
        return this.fileName;
    }
    
    /**
     * 写消息的回调接口
     */
    public interface AppendMessageCallback {

        /**
         * 序列化消息，写入 MappedByteBuffer
         */
        AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, 
        		final int maxBlank, final Message msg);
    }

}