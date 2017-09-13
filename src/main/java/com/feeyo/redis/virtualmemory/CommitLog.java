package com.feeyo.redis.virtualmemory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.virtualmemory.AppendMessageResult.AppendMessageStatus;
import com.feeyo.redis.virtualmemory.MappedFile.AppendMessageCallback;
import com.feeyo.redis.virtualmemory.PutMessageResult.PutMessageStatus;


// 用于存储真实的物理消息的结构
public class CommitLog {

	private static Logger log = LoggerFactory.getLogger(CommitLog.class);
	
	// Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = 0xAABBCCDD ^ 1880681586 + 8;
    
    // End of file empty MAGIC CODE cbd43194
    private final static int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;
    
   
	private final FlushCommitLogService flushCommitLogService;

	private final MappedFileQueue mappedFileQueue;
	private final AppendMessageCallback appendMessageCallback;

	private volatile long beginTimeInLock = 0;

	//true: Can lock, false : in lock.
    private AtomicBoolean _spinLock = new AtomicBoolean(true);
	
	private int commitLogFileSize;
    private int flushCommitLogLeastPages;
    private int flushCommitLogThoroughInterval;
	private boolean flushCommitLogTimed;
	private int flushIntervalCommitLog;

	/**
	 * CommitLog 是用于存储真实的物理消息的结构
	 */
	public CommitLog(String commitLogPath, int commitLogFileSize, 
			int flushCommitLogLeastPages, int flushCommitLogThoroughInterval, 
			boolean flushCommitLogTimed, int flushIntervalCommitLog,
			MappedFileAllocateService mappedFileAllocateService) {
		
		this.commitLogFileSize = commitLogFileSize;
		this.flushCommitLogLeastPages = flushCommitLogLeastPages;
		this.flushCommitLogThoroughInterval = flushCommitLogThoroughInterval;
		this.flushCommitLogTimed = flushCommitLogTimed;
		this.flushIntervalCommitLog = flushIntervalCommitLog;
		
		this.mappedFileQueue = new MappedFileQueue(commitLogPath, commitLogFileSize, mappedFileAllocateService);
		this.mappedFileQueue.load();
		
		this.flushCommitLogService = new FlushRealTimeService();		// 异步刷盘
		this.appendMessageCallback = new DefaultAppendMessageCallback();
	}

	public void start() {
		this.flushCommitLogService.start();
	}

	public void shutdown() {
		this.flushCommitLogService.shutdown();
	}
	
	public long flush() {
		this.mappedFileQueue.flush(0);
		return this.mappedFileQueue.getFlushedWhere();
	}
	
	public boolean resetOffset(long offset) {
		return this.mappedFileQueue.resetOffset(offset);
	}
	
	public long getMaxOffset() {
		return this.mappedFileQueue.getMaxOffset();
	}
	
    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }
	
    //删除commitLog里面过期的文件
	public int deleteExpiredFile(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly,
			final boolean cleanImmediately) {
		return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
	}
	
	public int deleteExpiredFile(long offset, final long intervalForcibly) {
	    return this.mappedFileQueue.deleteExpiredFileByOffset(offset, intervalForcibly);
	}
	
	public int deleteExpiredFile(final long intervalForcibly) {
	    return this.mappedFileQueue.deleteExpiredFileByConsumed(intervalForcibly);
	}
	

	public PutMessageResult putMessage(final Message msg) {
	
		AppendMessageResult result = null;
		MappedFile unlockMappedFile = null;
		MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
		
		while( !this._spinLock.compareAndSet(true, false) ) {
			// 写入锁, 自旋
		}

		try {
			long beginLockTimestamp = System.currentTimeMillis();
			this.beginTimeInLock = beginLockTimestamp;

			// 当不存在映射文件时，进行创建
			if (null == mappedFile || mappedFile.isFull()) {
				mappedFile = this.mappedFileQueue.getLastMappedFile(0);
			}
			
			if (null == mappedFile) {
				log.error("create maped file1 error");
				beginTimeInLock = 0;
				return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
			}

			// 存储消息
			msg.setStoreTimestamp( System.currentTimeMillis() ); 	//存储时间
			result = mappedFile.appendMessage(msg, this.appendMessageCallback);
			switch ( result.getStatus() ) {
			case PUT_OK:
				break;
			case END_OF_FILE: 
				unlockMappedFile = mappedFile;
				
				// Create a new file, re-write the message
				mappedFile = this.mappedFileQueue.getLastMappedFile(0);
				if (null == mappedFile) {
					beginTimeInLock = 0;
					return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
				}
				result = mappedFile.appendMessage(msg, this.appendMessageCallback);
				break;
			case MESSAGE_SIZE_EXCEEDED:
				beginTimeInLock = 0;
				return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
			case UNKNOWN_ERROR:
				beginTimeInLock = 0;
				return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
			default:
				beginTimeInLock = 0;
				return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
			}

			long eclipseTimeInLock = System.currentTimeMillis() - beginLockTimestamp;
			if (eclipseTimeInLock > 500) {
				log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}",
						new Object[] { eclipseTimeInLock, msg.getBody().length, result });
			}
			
			if ( null != unlockMappedFile ) {
	            unlockMappedFile.munlock();
	        }
			
			beginTimeInLock = 0;
			
		} finally {
			 // 释放写入锁
			 this._spinLock.compareAndSet(false, true);
		}

		// 唤醒 FlushRealTimeService 线程服务
		flushCommitLogService.wakeup();
	
		PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);
		return putMessageResult;
	}
	
	public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.commitLogFileSize;
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset & (mappedFileSize-1));
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }
	
	// 获取消息存储时间 msgStoreTime
	public long pickupStoreTimestamp(final long offset, final int size) {
		if (offset >= this.getMinOffset()) {
			SelectMappedBufferResult result = this.getMessage(offset, size);
			if (null != result) {
			    return result.getByteBuffer().getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
			}
		}
		return -1;
	}
	
	public long getMinOffset() {
		MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
		if (mappedFile != null) {
			if (mappedFile.isAvailable()) {
				return mappedFile.getFileFromOffset();
			} else {
				return this.rollNextFile(mappedFile.getFileFromOffset());
			}
		}
		return -1;
	}
	
	public long rollNextFile(final long offset) {
        int mappedFileSize = this.commitLogFileSize;
        return offset + mappedFileSize - offset % mappedFileSize;
    }
	
	public long getBeginTimeInLock() {
		return beginTimeInLock;
	}
	
    public void checkSelf() {
		mappedFileQueue.checkSelf();
	}
	
	public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }
	
	// 
	public void markAsConsumed(long offset, int size) {
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            //int readOffset = (int) (offset & (commitLogFileSize-1));
            int readOffset =  (int)( offset - mappedFile.getFileFromOffset() );
            mappedFile.markAsConsumed(readOffset, size);
        }
    }

	//
	//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	class DefaultAppendMessageCallback implements AppendMessageCallback {
		
		// 文件末尾空洞最小定长
		private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final int maxMessageSize = commitLogFileSize - END_FILE_MIN_BLANK_LENGTH;

		public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final Message msg) {
			
			// PHY OFFSET
            long wroteOffset = fileFromOffset + byteBuffer.position();
			
            // 
            final int bodyLength = msg.getBody() == null ? 0 : msg.getBody().length;
            final int msgLen = MessageDecoder.calMsgLength( bodyLength );
            
            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }
            
            // 确定是否有足够的空闲空间
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
            
                ByteBuffer endBuffer = ByteBuffer.allocate( END_FILE_MIN_BLANK_LENGTH + maxBlank );
                endBuffer.putInt(maxBlank);					   		// 1 TOTALSIZE
                endBuffer.putInt(CommitLog.BLANK_MAGIC_CODE);		// 2 MAGICCODE
                
                final long beginTimeMills = System.currentTimeMillis();

                byteBuffer.put( endBuffer.array(), 0, maxBlank );
                
                
                markAsConsumed(wroteOffset, maxBlank);
                
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msg.getStoreTimestamp(),
                		System.currentTimeMillis() - beginTimeMills);
            }
            
            
        	byte[] bb = MessageDecoder.encode(msg, false);
			long beginTimeMills = System.currentTimeMillis();
			// Write messages to the queue buffer
			byteBuffer.put(bb, 0, msgLen);

			AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen,
					msg.getStoreTimestamp(), System.currentTimeMillis() - beginTimeMills);
			return result;
		}
	}

	
	// 异步刷盘
	//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	abstract class FlushCommitLogService extends ServiceThread {
		protected static final int RETRY_TIMES_OVER = 10;
	}

	class FlushRealTimeService extends FlushCommitLogService {
		
		private long lastFlushTimestamp = 0;
		private long printTimes = 0;

		public void run() {
			log.info(this.getServiceName() + " service started");

			while (!this.isStopped()) {
				
				int interval = flushIntervalCommitLog;
				int flushPhysicQueueLeastPages = flushCommitLogLeastPages;
				int flushPhysicQueueThoroughInterval = flushCommitLogThoroughInterval;

				boolean printFlushProgress = false;

				// Print flush progress
				long currentTimeMillis = System.currentTimeMillis();
				if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
					this.lastFlushTimestamp = currentTimeMillis;
					flushPhysicQueueLeastPages = 0;
					printFlushProgress = (printTimes++ % 10) == 0;
				}

				try {
					if (flushCommitLogTimed) {
						Thread.sleep(interval);
					} else {
						this.waitForRunning(interval);
					}

					if (printFlushProgress) {
						this.printFlushProgress();
					}

					long begin = System.currentTimeMillis();
					
					mappedFileQueue.flush(flushPhysicQueueLeastPages);
					long past = System.currentTimeMillis() - begin;
					if (past > 500) {
						log.info("Flush data to disk costs {} ms", past);
					}
				} catch (Throwable e) {
					log.warn(this.getServiceName() + " service has exception. ", e);
					this.printFlushProgress();
				}
			}

			// Normal shutdown, to ensure that all the flush before exit
			boolean result = false;
			for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
				result = CommitLog.this.mappedFileQueue.flush(0);
				log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
			}

			this.printFlushProgress();
			log.info(this.getServiceName() + " service end");
		}

		@Override
		public String getServiceName() {
			return FlushRealTimeService.class.getSimpleName();
		}

		private void printFlushProgress() {
			// CommitLog.log.info("how much disk fall behind memory, "
			// + CommitLog.this.mappedFileQueue.howMuchFallBehind());
		}

		@Override
		public long getJointime() {
			return 1000 * 60 * 5;
		}
	}
	
}
