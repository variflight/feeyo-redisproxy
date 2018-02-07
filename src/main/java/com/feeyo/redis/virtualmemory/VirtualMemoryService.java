package com.feeyo.redis.virtualmemory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.virtualmemory.PutMessageResult.PutMessageStatus;
import com.feeyo.util.ThreadFactoryImpl;

/**
 * 存储层默认实现
 */
public class VirtualMemoryService  {
	
    private static final Logger log = LoggerFactory.getLogger(VirtualMemoryService.class);

    // 预分配MapedFile对象服务
    private final MappedFileAllocateService mappedFileAllocateService;
    
    // 清理物理文件服务
    private final CleanCommitLogService cleanCommitLogService;
    
    // 存储层的定时线程
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    
    // CommitLog
    private final CommitLog commitLog;
    
    // 存储服务是否启动
    private volatile boolean shutdown = true;
    
	private String commitLogPath = System.getProperty("log4jHome") + File.separator + "store" + File.separator + "commitlog";

	private int commitLogFileSize = 1024 * 1024 * 16;				// CommitLog 文件大小, default is 16M, 文件大小需要是 2^N bytes
    private int flushCommitLogLeastPages = 4;						// CommitLog，至少刷几个PAGE
    private int flushCommitLogThoroughInterval = 1000 * 10;			// CommitLog，彻底刷盘间隔时间 10秒
	private boolean flushCommitLogTimed = false;
	private int flushIntervalCommitLog = 300;						// CommitLog, 刷盘间隔时间（单位毫秒）
	
	private long osPageCacheBusyTimeOutMills = 1000;

    public VirtualMemoryService() throws IOException {
    	
    	// 确保目录干净
        this.ensureDirIsClean();
        
    	this.mappedFileAllocateService = new MappedFileAllocateService( commitLogFileSize );
    	this.mappedFileAllocateService.start();
    	
        this.commitLog = new CommitLog(commitLogPath, commitLogFileSize, flushCommitLogLeastPages, flushCommitLogThoroughInterval, 
        		flushCommitLogTimed, flushIntervalCommitLog, mappedFileAllocateService);

        this.cleanCommitLogService = new CleanCommitLogService();
    }
    
    private void ensureDirIsClean() throws IOException {
    	File file = new File(commitLogPath);
    	if ( file.exists()  ) {
    		
    		if ( file.isDirectory() ) {
	    		 String[] names = file.list();
	    		 for (String name : names) {
	    			   File f = new File(commitLogPath, name);
	                   if (f.isFile())
	                       f.delete();
	    		 }
    		} else {
    			log.error(commitLogPath + " already exists, but is not directory.");
    		}
    		
    	} else {
    		MappedFile.ensureDirOK( file.getParent() );
    		boolean result = file.mkdir();
	        log.info(commitLogPath + (result ? " create OK" : " already exists"));
    	}
    }


    public void start() throws IOException {
    	
        this.commitLog.start();
        

    	// Resource reclaim interval
        int cleanResourceInterval = 10_000;
        
        // 定时删除过期文件
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
            	// 定期清理文件
            	cleanCommitLogService.run();
            }
        }, 1000, cleanResourceInterval, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
            	// 定期自检
            	commitLog.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);
        
        
        this.shutdown = false;
    }

	public void shutdown() {
		if (!this.shutdown) {
			this.shutdown = true;
			
			this.scheduledExecutorService.shutdown();

			try {
				Thread.sleep(1000 * 3);
			} catch (InterruptedException e) {
				log.error("shutdown Exception, ", e);
			}

			this.commitLog.shutdown();
			this.mappedFileAllocateService.shutdown();
		}
	}

    public void destroy() {}

	public PutMessageResult putMessage(Message msg) {

        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
        
        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }
        
        long beginTime = System.currentTimeMillis();
        PutMessageResult result = this.commitLog.putMessage( msg);
        long eclipseTime = System.currentTimeMillis() - beginTime;
        if (eclipseTime > 500) {
        	log.warn("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBody().length);
        }
        return result;
	}
	
	public Message getMessage(long commitLogOffset, int size) {
		SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
		if (null != sbr) {
			try {
				return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
			} finally {
				sbr.release();
			}
		}
		return null;
	}
	
	public byte[] getMessageBodyAndMarkAsConsumed(long commitLogOffset, int size) {
		byte[] data =  getMessage(commitLogOffset, size).getBody();
		this.markAsConsumed(commitLogOffset, size);
		return data;
	}
    
    public void markAsConsumed(long offset, int size) {
        this.commitLog.markAsConsumed(offset, size);
    }
    
	//
	private boolean isOSPageCacheBusy() {
		long begin = this.commitLog.getBeginTimeInLock();
		long diff = System.currentTimeMillis() - begin;

		if (diff < 10000000 && diff > osPageCacheBusyTimeOutMills) {
			return true;
		}
		return false;
	}
	
	
	/*
	 清理物理文件服务
	 */
	class CleanCommitLogService {
		
		private long lastRedeleteTimestamp = 0;
		private long lastDeleteExpireTimestamp = 0;
		
		private long fileReservedTime = 10 * 60 * 1000;					// 保留间隔 10分钟
		private int deletePhysicFilesInterval = 100; 					// CommitLog removal interval
		private int destroyMappedFileIntervalForcibly = 1000 * 120;		//

		public void run() {
			try {
				this.deleteExpiredFiles();
				this.redeleteHangedFile();
			} catch (Throwable e) {
				log.warn(this.getServiceName() + " service has exception. ", e);
			}
		}
		
		private void deleteExpiredFiles() {

			// 删除已消费物理队列文件
			commitLog.deleteExpiredFile( destroyMappedFileIntervalForcibly );
			
			 // 删除超时的物理队列文件
			long currentTimestamp = System.currentTimeMillis();
			boolean timeup = ( currentTimestamp - lastDeleteExpireTimestamp  > fileReservedTime );
			boolean spacefull = this.isSpaceToDelete();
			if ( timeup || spacefull ) {
				
				this.lastDeleteExpireTimestamp = currentTimestamp;
				
				// 文件保留时间, 删除物理文件间隔, file强制销毁间隔，立即清理
				int deleteCount = commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval, 
						destroyMappedFileIntervalForcibly, spacefull);
				if (deleteCount <= 0 && spacefull) {
					log.warn("disk space will be full soon, but delete file failed.");
				}
			}
		}
		
		private boolean isSpaceToDelete() {
			// 检测物理文件磁盘空间
			double physicRatio = Util.getDiskPartitionSpaceUsedPercent( commitLogPath );	
			if (physicRatio > 0.65 ) {	
				log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
				return true;
			}
			return false;
		}

		// 最前面的文件有可能Hang住，定期检查一下
		private void redeleteHangedFile() {
			// 定期检查Hanged文件间隔时间（单位毫秒）120秒
			int interval = 1000 * 120;
			long currentTimestamp = System.currentTimeMillis();
			if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
				this.lastRedeleteTimestamp = currentTimestamp;
				int destroyMappedFileIntervalForcibly = 1000 * 120;
				if (commitLog.retryDeleteFirstFile( destroyMappedFileIntervalForcibly )) {
				}
			}
		}

		public String getServiceName() {
			return CleanCommitLogService.class.getSimpleName();
		}
	}
}
