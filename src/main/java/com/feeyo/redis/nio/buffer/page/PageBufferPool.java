package com.feeyo.redis.nio.buffer.page;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.nio.buffer.BufferPool;

import sun.nio.ch.DirectBuffer;

/**
 * DirectByteBuffer池，可以分配任意指定大小的DirectByteBuffer，用完需要归还
 * 
 * @author wuzhih
 * @author zagnix
 * @author zhuam
 */
public class PageBufferPool extends BufferPool {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(PageBufferPool.class);
    
    private ScheduledExecutorService expandExecutor = null;
    
    private PageBuffer[] allPages;
    private final int chunkSize;
    private AtomicInteger prevAllocatedPage;
    private final int pageSize;
    private int pageCount;
    
    private long sharedOptsCount;
    
    //
    private final AtomicBoolean checking = new AtomicBoolean( false );
    
    /**
     * 记录对线程ID->该线程的所使用Direct Buffer的size
     */
    private final ConcurrentHashMap<Long, Long> memoryUsage = new ConcurrentHashMap<Long, Long>();
    
    public PageBufferPool(long minBufferSize, final long maxBufferSize, int decomposeBufferSize,
    		int minChunkSize, int[] increments, int maxChunkSize) {
    	
    	super(minBufferSize, maxBufferSize, decomposeBufferSize, minChunkSize, increments, maxChunkSize);
    	
    	if (minChunkSize <= 0) {
    		this.chunkSize = increments[0];
    	} else {
    		this.chunkSize = minChunkSize;
    	}

    	this.pageCount = 10;
    	this.pageSize = (int) (minBufferSize / pageCount);
    	this.prevAllocatedPage = new AtomicInteger(0);
    	this.allPages = new PageBuffer[this.pageCount];
    	for (int i = 0; i < this.pageCount; i++) {
          allPages[i] = new PageBuffer(ByteBuffer.allocateDirect(pageSize), this.chunkSize);
      }
      
     
      if (maxBufferSize > minBufferSize  ) {
    	  
    	  // 自动扩容
    	   Runnable runable = new Runnable() {

				@Override
				public void run() {
					
					if ( !checking.compareAndSet( false,  true)) {
						return;
					}
					
					try {
						
						long usedMem = 0;
						for (Map.Entry<Long, Long> entry : memoryUsage.entrySet()) {
							usedMem += entry.getValue();
						}
						
						// 如果内存使用超过85%，扩充page
						long pageMem = pageSize * pageCount;
						long expectAllocateMem = usedMem + pageSize;
						
						// page 已使用百分比
						double usedPercentage = (double) usedMem / (double) pageMem;
						
						LOGGER.info("### bytebufferpage check ###  usedMem="+ usedMem + ", pageMem=" + pageMem + ", usedPercentage=" + usedPercentage);
						
						if ( usedPercentage > 0.85 && expectAllocateMem < maxBufferSize) {
							
							PageBuffer newPage = new PageBuffer(ByteBuffer.allocateDirect(pageSize), chunkSize);
							PageBuffer[] newAllPages = new PageBuffer[allPages.length + 1];
							for(int i =0 ; i < allPages.length; i++) {
								newAllPages[i] = allPages[i];
							}
							newAllPages[ newAllPages.length - 1] = newPage;
							allPages = newAllPages;
							pageCount++;
						}

					} catch (Exception e) {
						LOGGER.warn("bytebufferpage expand err:", e);

					} finally {
						checking.set(false);
					}
				}
    		   
    	   };
    	  
    	  // 如果配置的最大内存大于最小内存，启动线程监控
    	  expandExecutor = Executors.newSingleThreadScheduledExecutor();
    	  expandExecutor.scheduleAtFixedRate(runable, 120L, 300L, TimeUnit.SECONDS);
      }
    }

    @Override
	public ByteBuffer allocate(int size) {
		
		final int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
		int selectedPage = prevAllocatedPage.incrementAndGet() % allPages.length;
		ByteBuffer byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
		if (byteBuf == null) {
			byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
		}
		
		//
		final long threadId = Thread.currentThread().getId();
        if(byteBuf !=null){
            if (memoryUsage.containsKey(threadId)){
                memoryUsage.put(threadId, memoryUsage.get(threadId) + byteBuf.capacity());
            }else {
                memoryUsage.put(threadId, (long)byteBuf.capacity());
            }
        }

		if (byteBuf == null) {
			return ByteBuffer.allocate(size);
		}
		return byteBuf;
	}

    @Override
    public void recycle(ByteBuffer theBuf) {
    	
      	if(theBuf !=null && (!(theBuf instanceof DirectBuffer) )){
    		theBuf.clear();
    		return;
         }

      	final long size = theBuf.capacity();
		boolean recycled = false;
		DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
		int chunkCount = theBuf.capacity() / chunkSize;
		DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
		int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / chunkSize);
		for (int i = 0; i < allPages.length; i++) {
			if ((recycled = allPages[i].recycleBuffer((ByteBuffer) parentBuf, startChunk,chunkCount) == true)) {
				break;
			}
		}

		final long threadId = Thread.currentThread().getId();
		if (memoryUsage.containsKey(threadId)) {
			memoryUsage.put(threadId, memoryUsage.get(threadId) - size);
		}
		
		if (recycled == false) {
			LOGGER.warn("warning , not recycled buffer " + theBuf);
		}
		
		sharedOptsCount++;	
    }

    private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
        for (int i = startPage; i < endPage; i++) {
            ByteBuffer buffer = allPages[i].allocatChunk(theChunkCount);
            if (buffer != null) {
                prevAllocatedPage.getAndSet(i);
                return buffer;
            }
        }
        return null;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getPageCount() {
        return pageCount;
    }

    public long capacity() {
    	return this.maxBufferSize;
    }

    public long size(){
        return  (long) pageSize * pageCount;
    }
    
	@Override
	public long getSharedOptsCount() {
		return sharedOptsCount;
	}

    public PageBuffer[] getAllPages() {
		return allPages;
	}

    @Override
    public ConcurrentHashMap<Long,Long> getNetDirectMemoryUsage() {
        return memoryUsage;
    }
    
    //TODO 当页不够时，考虑扩展内存池的页的数量
    public  ByteBuffer expandBuffer(ByteBuffer buffer){
        int oldCapacity = buffer.capacity();
        int newCapacity = oldCapacity << 1;
        ByteBuffer newBuffer = allocate(newCapacity);
        if(newBuffer != null){
            int newPosition = buffer.position();
            buffer.flip();
            newBuffer.put(buffer);
            newBuffer.position(newPosition);
            recycle(buffer);
            return  newBuffer;
        }
        return null;
    }

}
